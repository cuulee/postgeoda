/**
 * Author: Xun Li <lixun910@gmail.com>
 *
 * Changes:
 * 2021-5-6 add pg_local_geary_window(), pg_local_multigeary_window()
 */


#include <postgres.h>
#include <pg_config.h>
#include <fmgr.h>
#include <nodes/execnodes.h>
#include <funcapi.h>
#include <windowapi.h>
#include <utils/array.h>
#include <catalog/pg_type.h>
#include <catalog/namespace.h>
#include <utils/geo_decls.h>
#include <utils/lsyscache.h> /* for get_typlenbyvalalign */

#ifdef __cplusplus
extern "C" {
#endif

#include <libgeoda/pg/utils.h>
#include <libgeoda/pg/geoms.h>
#include "proxy.h"
#include "lisa.h"

#ifndef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

Datum pg_local_geary_window(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_local_geary_window);
Datum pg_local_geary_window(PG_FUNCTION_ARGS) {
    WindowObject winobj = PG_WINDOW_OBJECT();
    lisa_context *context;
    int64 curpos, rowcount;

    Oid valsType = get_fn_expr_argtype(fcinfo->flinfo, 0);
    check_if_numeric_type(valsType);

    rowcount = WinGetPartitionRowCount(winobj);
    context = (lisa_context *)WinGetPartitionLocalMemory(winobj,sizeof(lisa_context) + sizeof(int) * rowcount);

    if (!context->isdone) {
        bool isnull, isout;

        /* We also need a non-zero N */
        int N = (int) WinGetPartitionRowCount(winobj);
        if (N <= 0) {
            context->isdone = true;
            context->isnull = true;
            PG_RETURN_NULL();
        }

        // read data
        uint8_t **w = lwalloc(sizeof(uint8_t *) * N);
        size_t *w_size = lwalloc(sizeof(size_t) * N);
        double *r = lwalloc(sizeof(double) * N);

        //lwdebug(1, "Init pg_local_g_window. N=%d", N);

        for (size_t i = 0; i < N; i++) {
            Datum arg = WinGetFuncArgInPartition(winobj, 0, i,
                                                 WINDOW_SEEK_HEAD, false, &isnull, &isout);
            r[i] = get_numeric_val(valsType, arg);
            Datum arg1 = WinGetFuncArgInPartition(winobj, 1, i,
                                                  WINDOW_SEEK_HEAD, false, &isnull, &isout);
            bytea *w_bytea = DatumGetByteaP(arg1); //shallow copy
            uint8_t *w_val = (uint8_t *) VARDATA(w_bytea);
            w[i] = w_val;
            w_size[i] = VARSIZE_ANY_EXHDR(w_bytea);
        }

        // read arguments
        int arg_index = 2;
        lisa_arguments args = {999, 0, 0.05, 6, 123456789};

        read_lisa_arguments(arg_index, PG_NARGS(), winobj, &args);

        // compute lisa
        lwdebug(1, "Enter pg_local_geary_window. N=%d", N);
        double **result = local_geary_window(N, r, (const uint8_t**)w, w_size, args.permutations, args.method,
                args.significance_cutoff, args.cpu_threads, args.seed);

        // Safe the result
        context->result = result;
        context->isdone = true;

        // Clean
        lwfree(r);
        lwfree(w);
        lwfree(w_size);

        lwdebug(1, "Exit pg_local_geary_window.");
    }

    if (context->isnull) {
        PG_RETURN_NULL();
    }

    curpos = WinGetCurrentPosition(winobj);

    // Wrap the results in a new PostgreSQL array object.
    double *p = context->result[curpos];
    Datum elems[3];
    elems[0] = Float8GetDatum(p[0]); // double to Datum
    elems[1] = Float8GetDatum(p[1]);
    elems[2] = Float8GetDatum(p[2]);
    free(p);

    int nelems = 3;
    Oid elmtype = FLOAT8OID;
    int16 elmlen;
    bool elmbyval;
    char elmalign;
    get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);
    ArrayType *array = construct_array(elems, nelems, elmtype, elmlen, elmbyval, elmalign);

    PG_RETURN_ARRAYTYPE_P(array);
}

Datum pg_local_multigeary_window(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_local_multigeary_window);
Datum pg_local_multigeary_window(PG_FUNCTION_ARGS) {
    WindowObject winobj = PG_WINDOW_OBJECT();
    lisa_context *context;
    int64 curpos, rowcount;

    rowcount = WinGetPartitionRowCount(winobj);
    context = (lisa_context *)WinGetPartitionLocalMemory(winobj,sizeof(lisa_context) + sizeof(int) * rowcount);

    if (!context->isdone) {
        bool isnull, isout;

        /* We also need a non-zero N */
        int N = (int) WinGetPartitionRowCount(winobj);
        if (N <= 0) {
            context->isdone = true;
            context->isnull = true;
            PG_RETURN_NULL();
        }

        // meta-data of input Array
        ArrayType *array;
        Oid arrayElementType;
        int16 arrayElementTypeWidth; //The array element type widths (should always be 4)
        // // The array element type "is passed by value" flags (not used, should always be true):
        bool arrayElementTypeByValue;
        // The array element type alignment codes (not used):
        char arrayElementTypeAlignmentCode;
        // The array contents, as PostgreSQL "datum" objects:
        Datum *arrayContent;
        // List of "is null" flags for the array contents:
        bool *arrayNullFlags;
        // The size of each array:
        int arrayLength;

        // read 1rd parameter: all the values from the partition window into a list
        // read 2th parameter: the weights in bytea from the parition window
        int idx_val = 0;
        int idx_wb = 1;

        // read data
        //Oid valsType = get_fn_expr_argtype(fcinfo->flinfo, idx_val);
        const uint8_t **w = lwalloc(sizeof(uint8_t *) * N);
        size_t *w_size = lwalloc(sizeof(size_t) * N);
        double **r = lwalloc(sizeof(double) * N);

        for (int i = 0; i < N; i++) {
            Datum arg_val = WinGetFuncArgInPartition(winobj, idx_val, i, WINDOW_SEEK_HEAD, false, &isnull, &isout);
            if (isnull) {
                PG_RETURN_NULL();
            }
            array = DatumGetArrayTypeP(arg_val);
            if (i == 0) {
                array = DatumGetArrayTypeP(arg_val);
                arrayElementType = ARR_ELEMTYPE(array);
                check_if_numeric_type(arrayElementType);
                get_typlenbyvalalign(arrayElementType, &arrayElementTypeWidth, &arrayElementTypeByValue,
                                     &arrayElementTypeAlignmentCode);
            }
            // Extract the array contents (as Datum objects).
            deconstruct_array(array, arrayElementType, arrayElementTypeWidth, arrayElementTypeByValue,
                              arrayElementTypeAlignmentCode, &arrayContent, &arrayNullFlags, &arrayLength);
            r[i] = lwalloc(sizeof(double) * arrayLength);
            for (size_t j = 0; j < arrayLength; ++j) {
                r[i][j] = get_numeric_val(arrayElementType, arrayContent[j]);
            }

            Datum arg_wb = WinGetFuncArgInPartition(winobj, idx_wb, i, WINDOW_SEEK_HEAD, false, &isnull, &isout);
            if (isnull) {
                PG_RETURN_NULL();
            }
            //bytea *w_bytea = DatumGetByteaP(arg1);
            bytea *w_bytea = (bytea*)PG_DETOAST_DATUM_COPY(arg_wb);
            uint8_t *w_val = (uint8_t *) VARDATA(w_bytea);
            w[i] = w_val;
            w_size[i] = VARSIZE_ANY_EXHDR(w_val);
        }

        // read arguments
        int arg_index = 2;
        lisa_arguments args = {999, 0, 0.05, 6, 123456789};

        read_lisa_arguments(arg_index, PG_NARGS(), winobj, &args);

        // compute lisa
        lwdebug(1, "Enter pg_local_multigeary_window. N=%d", N);
        double **result = local_multigeary_window(arrayLength, N, (const double**)r, (const uint8_t**)w,
                w_size, args.permutations, args.method, args.significance_cutoff, args.cpu_threads, args.seed);

        // Safe the result
        context->result = result;
        context->isdone = true;

        // Clean
        for (int i=0; i<N; ++i) lwfree(r[i]);
        lwfree(r);
        lwfree(w);
        lwfree(w_size);

        lwdebug(1, "Exit pg_local_multigeary_window. done.");
    }

    if (context->isnull) {
        PG_RETURN_NULL();
    }

    curpos = WinGetCurrentPosition(winobj);

    // Wrap the results in a new PostgreSQL array object.
    double *p = context->result[curpos];
    Datum elems[3];
    elems[0] = Float8GetDatum(p[0]); // double to Datum
    elems[1] = Float8GetDatum(p[1]);
    elems[2] = Float8GetDatum(p[2]);
    free(p);

    int nelems = 3;
    Oid elmtype = FLOAT8OID;
    int16 elmlen;
    bool elmbyval;
    char elmalign;
    get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);
    ArrayType *array = construct_array(elems, nelems, elmtype, elmlen, elmbyval, elmalign);

    PG_RETURN_ARRAYTYPE_P(array);
}

#ifdef __cplusplus
}
#endif

