/**
 * Author: Xun Li <lixun910@gmail.com>
 *
 * Changes:
 * 2021-1-29 add local_g_window_bytea() local_gstar_window_bytea()
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

#include <libgeoda/pg/config.h>
#include <libgeoda/pg/geoms.h>
#include "proxy.h"
#include "lisa.h"

#ifndef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

Datum local_g_window_bytea(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(local_g_window_bytea);
Datum local_g_window_bytea(PG_FUNCTION_ARGS) {
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

        // read 2nd parameter weights in bytea
        //bytea *bw = DatumGetByteaP(WinGetFuncArgCurrent(winobj, 1, &isnull));
        Datum arg_b = WinGetFuncArgCurrent(winobj, 2, &isnull);
        bytea *bw = (bytea*)PG_DETOAST_DATUM_COPY(arg_b);
        if (isnull) {
            PG_RETURN_NULL();
        }
        uint8_t *w = (uint8_t*)VARDATA(bw); // NOTE memory?

        // read 1st parameter fids
        int64 *fids = lwalloc(sizeof(int64) * N);
        for (size_t i = 0; i < N; i++) {
            Datum arg = WinGetFuncArgInPartition(winobj, 0, i,
                                                 WINDOW_SEEK_HEAD, false, &isnull, &isout);
            fids[i] = DatumGetInt64(arg);
            //lwdebug(1, "local_g_window_bytea: %d-th:%d", i, fids[i]);
        }

        // Read all the values from the partition window into a list
        Oid valsType = get_fn_expr_argtype(fcinfo->flinfo, 1);
        check_if_numeric_type(valsType);

        double *r = lwalloc(sizeof(double) * N);
        for (size_t i = 0; i < N; i++) {
            Datum arg = WinGetFuncArgInPartition(winobj, 1, i,
                                                 WINDOW_SEEK_HEAD, false, &isnull, &isout);
            r[i] = get_numeric_val(valsType, arg);
            //lwdebug(1, "local_g_window_bytea: %d-th:%f", i, r[i]);
        }

        // compute lisa
        Point **result = pg_local_g(N, fids, r, w);

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

Datum local_gstar_window_bytea(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(local_gstar_window_bytea);
Datum local_gstar_window_bytea(PG_FUNCTION_ARGS) {
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

        // read 2nd parameter weights in bytea
        //bytea *bw = DatumGetByteaP(WinGetFuncArgCurrent(winobj, 1, &isnull));
        Datum arg_b = WinGetFuncArgCurrent(winobj, 2, &isnull);
        bytea *bw = (bytea*)PG_DETOAST_DATUM_COPY(arg_b);
        if (isnull) {
            PG_RETURN_NULL();
        }
        uint8_t *w = (uint8_t*)VARDATA(bw); // NOTE memory?

        // read 1st parameter fids
        int64 *fids = lwalloc(sizeof(int64) * N);
        for (size_t i = 0; i < N; i++) {
            Datum arg = WinGetFuncArgInPartition(winobj, 0, i,
                                                 WINDOW_SEEK_HEAD, false, &isnull, &isout);
            fids[i] = DatumGetInt64(arg);
            //lwdebug(1, "local_gstar_window_bytea: %d-th:%d", i, fids[i]);
        }

        // Read all the values from the partition window into a list
        Oid valsType = get_fn_expr_argtype(fcinfo->flinfo, 1);
        check_if_numeric_type(valsType);

        double *r = lwalloc(sizeof(double) * N);
        for (size_t i = 0; i < N; i++) {
            Datum arg = WinGetFuncArgInPartition(winobj, 1, i,
                                                 WINDOW_SEEK_HEAD, false, &isnull, &isout);
            r[i] = get_numeric_val(valsType, arg);
            //lwdebug(1, "local_gstar_window_bytea: %d-th:%f", i, r[i]);
        }

        // compute lisa
        Point **result = pg_local_gstar(N, fids, r, w);

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

#ifdef __cplusplus
}
#endif

