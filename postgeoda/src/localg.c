/**
 * Author: Xun Li <lixun910@gmail.com>
 *
 * Changes:
 * 2021-1-29 add local_g_window_bytea() local_gstar_window_bytea()
 * 2021-4-28 remove old function using weights as a whole; change to pg_local_g_window(), pg_local_gstar_window();
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

/**
 * pg_local_g_window()
 *
 * This function is for Window SQL function local_g()
 *
 * @param fcinfo
 * @return
 */
Datum pg_local_g_window(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pg_local_g_window);
Datum pg_local_g_window(PG_FUNCTION_ARGS) {
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

        double **result = local_g_window(N, r, (const uint8_t**)w, w_size, args.permutations, args.method,
                                             args.significance_cutoff, args.cpu_threads, args.seed);

        // Safe the result
        context->result = result;
        context->isdone = true;

        // clean
        lwdebug(1, "Clean pg_local_g_window.");
        lwfree(r);
        lwfree(w_size);
        lwfree(w);

        lwdebug(1, "Exit pg_local_g_window.");
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

/**
 * pg_local_gstar_window()
 *
 * This function is for Window SQL function local_gstar()
 *
 * @param fcinfo
 * @return
 */
Datum pg_local_gstar_window(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pg_local_gstar_window);
Datum pg_local_gstar_window(PG_FUNCTION_ARGS) {
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

        //lwdebug(1, "Init pg_local_gstar_window. N=%d", N);

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

        double **result = local_gstar_window(N, r, (const uint8_t**)w, w_size, args.permutations, args.method,
                                             args.significance_cutoff, args.cpu_threads, args.seed);

        // Safe the result
        context->result = result;
        context->isdone = true;

        // clean
        lwdebug(1, "Clean pg_local_gstar_window.");
        lwfree(r);
        lwfree(w_size);
        lwfree(w);

        lwdebug(1, "Exit pg_local_gstar_window.");
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

