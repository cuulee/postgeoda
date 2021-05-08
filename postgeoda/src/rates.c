/**
 * Author: Xun Li <lixun910@gmail.com>
 *
 * Changes:
 * 2021-5-7 add pg_excess_risk()
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
 * rates_context
 *
 * Data structure used in LISA Windows functions to return results
 */
typedef struct {
    bool	isdone;
    bool	isnull;
    double   *result;
    /* variable length */
} rates_context;

Datum pg_excess_risk(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_excess_risk);
Datum pg_excess_risk(PG_FUNCTION_ARGS) {
    WindowObject winobj = PG_WINDOW_OBJECT();
    rates_context *context;
    int64 curpos, rowcount;

    Oid valsType = get_fn_expr_argtype(fcinfo->flinfo, 0);
    check_if_numeric_type(valsType);

    rowcount = WinGetPartitionRowCount(winobj);
    context = (rates_context *)WinGetPartitionLocalMemory(winobj,sizeof(rates_context) + sizeof(int) * rowcount);

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
        double *e = lwalloc(sizeof(double) * N);
        double *b = lwalloc(sizeof(double) * N);

        lwdebug(0, "Init pg_excess_risk. N=%d", N);

        for (size_t i = 0; i < N; i++) {
            Datum arg = WinGetFuncArgInPartition(winobj, 0, i,
                                                 WINDOW_SEEK_HEAD, false, &isnull, &isout);
            e[i] = get_numeric_val(valsType, arg);
            Datum arg1 = WinGetFuncArgInPartition(winobj, 1, i,
                                                  WINDOW_SEEK_HEAD, false, &isnull, &isout);
            b[i] = get_numeric_val(valsType, arg1);
        }


        // compute lisa
        lwdebug(1, "Enter pg_excess_risk. N=%d", N);
        double *result = excess_risk_window(N, e, b);

        // Safe the result
        context->result = result;
        context->isdone = true;

        // Clean
        lwfree(e);
        lwfree(b);

        lwdebug(1, "Exit pg_excess_risk.");
    }

    if (context->isnull) {
        PG_RETURN_NULL();
    }

    curpos = WinGetCurrentPosition(winobj);

    PG_RETURN_FLOAT8(context->result[curpos]);
}


#ifdef __cplusplus
}
#endif

