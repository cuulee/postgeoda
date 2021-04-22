/**
 * Author: Xun Li <lixun910@gmail.com>
 *
 * Changes:
 * 2021-1-27 add local_joincount_window_bytea
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

Datum local_joincount_window_bytea(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(local_joincount_window_bytea);
Datum local_joincount_window_bytea(PG_FUNCTION_ARGS) {
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

        // read weights in bytea
        //bytea *bw = DatumGetByteaP(WinGetFuncArgCurrent(winobj, 1, &isnull));
        Datum arg_b = WinGetFuncArgCurrent(winobj, 2, &isnull);
        bytea *bw = (bytea*)PG_DETOAST_DATUM_COPY(arg_b);
        if (isnull) {
            PG_RETURN_NULL();
        }
        uint8_t *w = (uint8_t*)VARDATA(bw); // NOTE memory?

        // read fids
        int64 *fids = lwalloc(sizeof(int64) * N);
        for (size_t i = 0; i < N; i++) {
            Datum arg = WinGetFuncArgInPartition(winobj, 0, i,
                                                 WINDOW_SEEK_HEAD, false, &isnull, &isout);
            fids[i] = DatumGetInt64(arg);
            //lwdebug(1, "local_joincount_window_bytea: %d-th:%d", i, fids[i]);
        }

        // Read all the values from the partition window into a list
        Oid valsType = get_fn_expr_argtype(fcinfo->flinfo, 1);
        check_if_numeric_type(valsType);

        double *r = lwalloc(sizeof(double) * N);
        for (size_t i = 0; i < N; i++) {
            Datum arg = WinGetFuncArgInPartition(winobj, 1, i,
                                                 WINDOW_SEEK_HEAD, false, &isnull, &isout);
            r[i] = get_numeric_val(valsType, arg);
            //lwdebug(1, "local_joincount_window_bytea: %d-th:%f", i, r[i]);
        }

        // compute lisa
        Point **result = pg_local_joincount(N, fids, r, w);

        // Safe the result
        context->result = result;
        context->isdone = true;

        // Clean
        lwfree(r);
        PG_FREE_IF_COPY(bw, 0);

        lwdebug(1, "Exit local_joincount_window_bytea. free_lisa() done.");
    }

    if (context->isnull)
        PG_RETURN_NULL();

    curpos = WinGetCurrentPosition(winobj);
    PG_RETURN_POINT_P(context->result[curpos]);
}


Datum local_bjoincount_window_bytea(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(local_bjoincount_window_bytea);
Datum local_bjoincount_window_bytea(PG_FUNCTION_ARGS) {
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

        // 3rd argument: read weights in bytea
        //bytea *bw = DatumGetByteaP(WinGetFuncArgCurrent(winobj, 1, &isnull));
        Datum arg_b = WinGetFuncArgCurrent(winobj, 3, &isnull);
        bytea *bw = (bytea*)PG_DETOAST_DATUM_COPY(arg_b);
        if (isnull) {
            PG_RETURN_NULL();
        }
        uint8_t *w = (uint8_t*)VARDATA(bw); // NOTE memory?

        // 1st argument: read fids
        int64 *fids = lwalloc(sizeof(int64) * N);
        for (size_t i = 0; i < N; i++) {
            Datum arg = WinGetFuncArgInPartition(winobj, 0, i,
                                                 WINDOW_SEEK_HEAD, false, &isnull, &isout);
            fids[i] = DatumGetInt64(arg);
            //lwdebug(1, "local_bjoincount_window_bytea: %d-th:%d", i, fids[i]);
        }

        // Read all the values from the partition window into a list
        Oid valsType1 = get_fn_expr_argtype(fcinfo->flinfo, 1);
        check_if_numeric_type(valsType1);

        double *r1 = lwalloc(sizeof(double) * N);
        for (size_t i = 0; i < N; i++) {
            Datum arg = WinGetFuncArgInPartition(winobj, 1, i,
                                                 WINDOW_SEEK_HEAD, false, &isnull, &isout);
            r1[i] = get_numeric_val(valsType1, arg);
            //lwdebug(1, "local_bjoincount_window_bytea: %d-th:%f", i, r1[i]);
        }

        Oid valsType2 = get_fn_expr_argtype(fcinfo->flinfo, 2);
        check_if_numeric_type(valsType2);

        double *r2 = lwalloc(sizeof(double) * N);
        for (size_t i = 0; i < N; i++) {
            Datum arg = WinGetFuncArgInPartition(winobj, 2, i,
                                                 WINDOW_SEEK_HEAD, false, &isnull, &isout);
            r2[i] = get_numeric_val(valsType2, arg);
            //lwdebug(1, "local_bjoincount_window_bytea: %d-th:%f", i, r2[i]);
        }

        // compute lisa
        Point **result = pg_bivariate_local_joincount(N, fids, r1, r2, w);

        // Safe the result
        context->result = result;
        context->isdone = true;

        // Clean
        lwfree(r1);
        lwfree(r2);
        PG_FREE_IF_COPY(bw, 0);

        lwdebug(1, "Exit local_bjoincount_window_bytea. free_lisa() done.");
    }

    if (context->isnull)
        PG_RETURN_NULL();

    curpos = WinGetCurrentPosition(winobj);
    PG_RETURN_POINT_P(context->result[curpos]);
}


Datum local_mjoincount_window_bytea(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(local_mjoincount_window_bytea);
Datum local_mjoincount_window_bytea(PG_FUNCTION_ARGS) {
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

        // read weights in bytea
        //bytea *bw = DatumGetByteaP(WinGetFuncArgCurrent(winobj, 1, &isnull));
        Datum arg_b = WinGetFuncArgCurrent(winobj, 2, &isnull);
        bytea *bw = (bytea*)PG_DETOAST_DATUM_COPY(arg_b);
        if (isnull) {
            PG_RETURN_NULL();
        }
        uint8_t *w = (uint8_t*)VARDATA(bw); // NOTE memory?

        // read fids
        int64 *fids = lwalloc(sizeof(int64) * N);
        for (size_t i = 0; i < N; i++) {
            Datum arg = WinGetFuncArgInPartition(winobj, 0, i,
                                                 WINDOW_SEEK_HEAD, false, &isnull, &isout);
            fids[i] = DatumGetInt64(arg);
            //lwdebug(1, "local_mjoincount_window_bytea: %d-th:%d", i, fids[i]);
        }

        // Read all the values from the partition window into a list
        ArrayType *currentArray = PG_GETARG_ARRAYTYPE_P(1);
        Oid elemTypeId = ARR_ELEMTYPE(currentArray);
        check_if_numeric_type(elemTypeId);

        Oid valsType = get_fn_expr_argtype(fcinfo->flinfo, 1);
        check_if_numeric_type(valsType);

        double *r = lwalloc(sizeof(double) * N);
        for (size_t i = 0; i < N; i++) {
            Datum arg = WinGetFuncArgInPartition(winobj, 1, i,
                                                 WINDOW_SEEK_HEAD, false, &isnull, &isout);
            r[i] = get_numeric_val(valsType, arg);
            //lwdebug(1, "local_mjoincount_window_bytea: %d-th:%f", i, r[i]);
        }

        // compute lisa
        Point **result = pg_local_joincount(N, fids, r, w);

        // Safe the result
        context->result = result;
        context->isdone = true;

        // Clean
        lwfree(r);
        PG_FREE_IF_COPY(bw, 0);

        lwdebug(1, "Exit local_mjoincount_window_bytea. free_lisa() done.");
    }

    if (context->isnull)
        PG_RETURN_NULL();

    curpos = WinGetCurrentPosition(winobj);
    PG_RETURN_POINT_P(context->result[curpos]);
}

#ifdef __cplusplus
}
#endif