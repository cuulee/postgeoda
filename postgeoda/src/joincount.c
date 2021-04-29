/**
 * Author: Xun Li <lixun910@gmail.com>
 *
 * Changes:
 * 2021-1-27 add local_joincount_window_bytea
 * 2021-4-28 change to pg_local_joincount_window(), add pg_local_bijoincount_window(),
 * add pg_local_multijoincount_window()
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

Datum pg_local_joincount_window(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pg_local_joincount_window);
Datum pg_local_joincount_window(PG_FUNCTION_ARGS) {
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

        lwdebug(0, "Init pg_local_joincount_window. N=%d", N);

        for (size_t i = 0; i < N; i++) {
            Datum arg = WinGetFuncArgInPartition(winobj, 0, i,
                                                 WINDOW_SEEK_HEAD, false, &isnull, &isout);
            if (isnull) {
                PG_RETURN_NULL();
            }
            r[i] = get_numeric_val(valsType, arg);
            Datum arg1 = WinGetFuncArgInPartition(winobj, 1, i,
                                                  WINDOW_SEEK_HEAD, false, &isnull, &isout);
            if (isnull) {
                PG_RETURN_NULL();
            }
            bytea *w_bytea = DatumGetByteaP(arg1); //shallow copy
            //bytea *w_bytea = (bytea*)PG_DETOAST_DATUM_COPY(arg1);
            uint8_t *w_val = (uint8_t *) VARDATA(w_bytea);
            w[i] = w_val;
            w_size[i] = VARSIZE_ANY_EXHDR(w_bytea);
        }

        // read arguments
        int arg_index = 2;
        lisa_arguments args = {999, 0, 0.05, 6, 123456789};

        read_lisa_arguments(arg_index, PG_NARGS(), winobj, &args);

        double **result = local_joincount_window(N, r, (const uint8_t**)w, w_size, args.permutations, args.method,
                                         args.significance_cutoff, args.cpu_threads, args.seed);

        // Safe the result
        context->result = result;
        context->isdone = true;

        // Clean
        lwdebug(1, "Clean pg_local_joincount_window.");
        lwfree(r);
        lwfree(w_size);
        lwfree(w);

        lwdebug(1, "Exit pg_local_joincount_window. free_lisa() done.");
    }

    if (context->isnull)
        PG_RETURN_NULL();

    curpos = WinGetCurrentPosition(winobj);

    // Wrap the results in a new PostgreSQL array object.
    double *p = context->result[curpos];
    Datum elems[3];
    elems[0] = Float8GetDatum(p[0]); // double to Datum
    elems[1] = Float8GetDatum(p[1]);
    elems[2] = Float8GetDatum(p[2]);
    int nelems = 3;
    Oid elmtype = FLOAT8OID;
    int16 elmlen;
    bool elmbyval;
    char elmalign;
    get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);
    ArrayType *array = construct_array(elems, nelems, elmtype, elmlen, elmbyval, elmalign);

    PG_RETURN_ARRAYTYPE_P(array);
}


Datum pg_local_bijoincount_window(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pg_local_bijoincount_window);
Datum pg_local_bijoincount_window(PG_FUNCTION_ARGS) {
    WindowObject winobj = PG_WINDOW_OBJECT();
    lisa_context *context;
    int64 curpos, rowcount;

    Oid valsType1 = get_fn_expr_argtype(fcinfo->flinfo, 0);
    check_if_numeric_type(valsType1);

    Oid valsType2 = get_fn_expr_argtype(fcinfo->flinfo, 1);
    check_if_numeric_type(valsType2);

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
        double *r1 = lwalloc(sizeof(double) * N);
        double *r2 = lwalloc(sizeof(double) * N);

        lwdebug(0, "Init pg_local_bijoincount_window. N=%d", N);

        for (size_t i = 0; i < N; i++) {
            Datum arg1 = WinGetFuncArgInPartition(winobj, 0, i,
                                                 WINDOW_SEEK_HEAD, false, &isnull, &isout);
            if (isnull) {
                PG_RETURN_NULL();
            }
            r1[i] = get_numeric_val(valsType1, arg1);
            Datum arg2 = WinGetFuncArgInPartition(winobj, 1, i,
                                                  WINDOW_SEEK_HEAD, false, &isnull, &isout);
            if (isnull) {
                PG_RETURN_NULL();
            }
            r2[i] = get_numeric_val(valsType1, arg2);
            Datum arg3 = WinGetFuncArgInPartition(winobj, 2, i,
                                                  WINDOW_SEEK_HEAD, false, &isnull, &isout);
            if (isnull) {
                PG_RETURN_NULL();
            }
            bytea *w_bytea = DatumGetByteaP(arg3); //shallow copy
            uint8_t *w_val = (uint8_t *) VARDATA(w_bytea);
            w[i] = w_val;
            w_size[i] = VARSIZE_ANY_EXHDR(w_bytea);
        }

        // read arguments
        int arg_index = 3;
        lisa_arguments args = {999, 0, 0.05, 6, 123456789};

        read_lisa_arguments(arg_index, PG_NARGS(), winobj, &args);

        double **result = local_bijoincount_window(N, r1, r2, (const uint8_t**)w, w_size, args.permutations,
                                                   args.method, args.significance_cutoff, args.cpu_threads, args.seed);

        // Safe the result
        context->result = result;
        context->isdone = true;

        // Clean
        lwdebug(1, "Clean pg_local_bijoincount_window.");
        lwfree(r1);
        lwfree(r2);
        lwfree(w_size);
        lwfree(w);

        lwdebug(1, "Exit pg_local_bijoincount_window. free_lisa() done.");
    }

    if (context->isnull)
        PG_RETURN_NULL();

    curpos = WinGetCurrentPosition(winobj);

    // Wrap the results in a new PostgreSQL array object.
    double *p = context->result[curpos];
    Datum elems[3];
    elems[0] = Float8GetDatum(p[0]); // double to Datum
    elems[1] = Float8GetDatum(p[1]);
    elems[2] = Float8GetDatum(p[2]);
    int nelems = 3;
    Oid elmtype = FLOAT8OID;
    int16 elmlen;
    bool elmbyval;
    char elmalign;
    get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);
    ArrayType *array = construct_array(elems, nelems, elmtype, elmlen, elmbyval, elmalign);

    PG_RETURN_ARRAYTYPE_P(array);
}


Datum pg_local_multijoincount_window(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pg_local_multijoincount_window);
Datum pg_local_multijoincount_window(PG_FUNCTION_ARGS) {
    WindowObject winobj = PG_WINDOW_OBJECT();
    lisa_context *context;
    int64 curpos, rowcount;

    rowcount = WinGetPartitionRowCount(winobj);
    context = (lisa_context *)WinGetPartitionLocalMemory(winobj,
                                                         sizeof(lisa_context) + sizeof(int) * rowcount);

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
        double **r =  lwalloc(sizeof(double) * N);

        // get meta-data of input Array
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

        for (size_t i = 0; i < N; i++) {
            Datum arg0 = WinGetFuncArgInPartition(winobj, 0, i,
                                                  WINDOW_SEEK_HEAD, false, &isnull, &isout);
            if (isnull) {
                PG_RETURN_NULL();
            }
            array = DatumGetArrayTypeP(arg0);
            if (i == 0) {
                array = DatumGetArrayTypeP(arg0);
                //arrayLength = (ARR_DIMS(array))[0];
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

            Datum arg1 = WinGetFuncArgInPartition(winobj, 1, i,
                                                  WINDOW_SEEK_HEAD, false, &isnull, &isout);
            if (isnull) {
                PG_RETURN_NULL();
            }
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
        double **result = local_multijoincount_window(N, arrayLength, (const double**)r, (const uint8_t**)w, w_size,
                                                      args.permutations, args.method, args.significance_cutoff,
                                                      args.cpu_threads, args.seed);

        // Safe the result
        context->result = result;
        context->isdone = true;

        // Clean
        for (int i=0; i<N; ++i) lwfree(r[i]);
        lwfree(r);
        lwfree(w_size);
        lwfree(w);

        lwdebug(1, "Exit pg_local_multijoincount_window. free_lisa() done.");
    }

    if (context->isnull)
        PG_RETURN_NULL();

    curpos = WinGetCurrentPosition(winobj);

    // Wrap the results in a new PostgreSQL array object.
    // Wrap the results in a new PostgreSQL array object.
    double *p = context->result[curpos];
    Datum elems[3];
    elems[0] = Float8GetDatum(p[0]); // double to Datum
    elems[1] = Float8GetDatum(p[1]);
    elems[2] = Float8GetDatum(p[2]);
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