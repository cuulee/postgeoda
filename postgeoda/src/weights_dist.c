/**
 * Author: Xun Li <lixun910@gmail.com>
 *
 * Changes:
 * 2021-4-23 Add pg_distance_weights_window()
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

#include <libgeoda/pg/geoms.h>
#include "proxy.h"

#include "weights.h"

#ifndef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif


/**
 * distance_context
 *
 * The structure used in function `pg_distance_weights_window` to construct
 * returning results of distance-based spatial weights.
 *
 */
typedef struct {
    bool	isdone;
    bool	isnull;
    bytea **result;
    /* variable length */
} distance_context;

/**
 * Window function for SQL `distance_weights()`
 * This will be the main interface for knn weights.
 *
 * @param fcinfo
 * @return
 */
Datum pg_distance_weights_window(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_distance_weights_window);
Datum pg_distance_weights_window(PG_FUNCTION_ARGS) {
    //this function will be called multiple times until context->isdone==true
    //lwdebug(1, "Enter pg_knn_weights.");

    WindowObject winobj = PG_WINDOW_OBJECT();
    knn_context *context;
    int64 curpos, rowcount;

    rowcount = WinGetPartitionRowCount(winobj);
    context = (knn_context *)WinGetPartitionLocalMemory(winobj, sizeof(KnnCollectionState) + sizeof(int) * rowcount);

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
        List *geoms;
        List *fids;

        for (size_t i = 0; i < N; i++) {
            // fid
            Datum arg = WinGetFuncArgInPartition(winobj, 0, i, WINDOW_SEEK_HEAD, false, &isnull, &isout);
            int64 fid = DatumGetInt64(arg);
            //lwdebug(1, "local_g_window_bytea: %d-th:%d", i, fids[i]);

            // the_geom
            Datum arg1 = WinGetFuncArgInPartition(winobj, 1, i, WINDOW_SEEK_HEAD, false, &isnull, &isout);
            bytea *bytea_wkb = (bytea*)PG_DETOAST_DATUM_COPY(arg1);
            uint8_t *wkb = (uint8_t *) VARDATA(bytea_wkb);
            LWGEOM *lwgeom = lwgeom_from_wkb(wkb, VARSIZE_ANY_EXHDR(bytea_wkb), LW_PARSER_CHECK_ALL);
            if (i==0) {
                geoms = list_make1(lwgeom);
                fids = list_make1_int(fid);
            } else {
                lappend(geoms, lwgeom);
                lappend_int(fids, fid);
            }
        }

        // read arguments: dist_thres, power, is_inverse, is_arc, is_mile
        double dist_thres = 0.0;
        if (PG_ARGISNULL(2)) {
            dist_thres = DatumGetFloat4(WinGetFuncArgCurrent(winobj, 2, &isnull));
            if (isnull || dist_thres <= 0) {
                PG_RETURN_NULL();
            }
        }

        double power = 1.0;
        if (PG_ARGISNULL(3)) {
            power = DatumGetFloat4(WinGetFuncArgCurrent(winobj, 3, &isnull));
            if (isnull || power <= 0) {
                power = 1.0;
            }
        }

        bool is_inverse = false;
        if (PG_ARGISNULL(4)) {
            is_inverse = DatumGetBool(WinGetFuncArgCurrent(winobj, 4, &isnull));
        }

        bool is_arc = false;
        if (PG_ARGISNULL(5)) {
            is_arc = DatumGetBool(WinGetFuncArgCurrent(winobj, 5, &isnull));
        }

        bool is_mile = false;
        if (PG_ARGISNULL(6)) {
            is_mile = DatumGetBool(WinGetFuncArgCurrent(winobj, 6, &isnull));
        }

        // create weights
        PGWeight* w = create_distance_weights(fids, geoms, dist_thres, power, is_inverse, is_arc, is_mile);
        bytea **result = weights_to_bytea_array(w);

        // Safe the result
        context->result = result;
        context->isdone = true;

        // free PGWeight
        free_pgweight(w);

        lwdebug(1, "Exit pg_knn_weights. done.");
    }

    if (context->isnull) {
        PG_RETURN_NULL();
    }

    curpos = WinGetCurrentPosition(winobj);
    PG_RETURN_BYTEA_P(context->result[curpos]);
}

#ifdef __cplusplus
}
#endif