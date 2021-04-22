/**
 * Author: Xun Li <lixun910@gmail.com>
 *
 * Changes:
 * 2021-4-9 add pg_quantilelisa()
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

Datum quantilelisa_window(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(quantilelisa_window);

/**
 * C function for SQL: geoda_quantilelisa(integer, integer, anyelement, bytea)
 *
 * @param fcinfo
 * @return
 */
Datum quantilelisa_window(PG_FUNCTION_ARGS) {
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

        // read 1st parameter fids
        int idx_k = 0;
        Datum arg_k = WinGetFuncArgCurrent(winobj, idx_k, &isnull);
        int64 k = DatumGetInt64(arg_k);

        // read 2nd parameter fids
        int idx_q = 1;
        Datum arg_q = WinGetFuncArgCurrent(winobj, idx_q, &isnull);
        int64 q = DatumGetInt64(arg_q);

        // read 3rd parameter: all the values from the partition window into a list
        // read 4th parameter: the weights in bytea from the parition window
        int idx_val = 2;
        int idx_wb = 3;

        Oid valsType = get_fn_expr_argtype(fcinfo->flinfo, idx_val);
        const uint8_t **w = lwalloc(sizeof(uint8_t *) * N);
        size_t *w_size = lwalloc(sizeof(size_t) * N);
        double *r = lwalloc(sizeof(double) * N);

        for (int i = 0; i < N; i++) {
            Datum arg_val = WinGetFuncArgInPartition(winobj, idx_val, i, WINDOW_SEEK_HEAD, false, &isnull, &isout);
            if (isnull) {
                PG_RETURN_NULL();
            }
            r[i] = get_numeric_val(valsType, arg_val);
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

        // compute lisa
        lwdebug(1, "Enter quantilelisa_window. N=%d", N);
        Point **result = pg_quantilelisa(k, q, N, r, w, w_size);

        // Safe the result
        context->result = result;
        context->isdone = true;

        // Clean
        lwfree(r);
        lwfree(w);
        lwfree(w_size);

        lwdebug(1, "Exit quantilelisa_window. free_lisa() done.");
    }

    if (context->isnull) {
        PG_RETURN_NULL();
    }

    curpos = WinGetCurrentPosition(winobj);
    PG_RETURN_POINT_P(context->result[curpos]);
}

#ifdef __cplusplus
}
#endif

