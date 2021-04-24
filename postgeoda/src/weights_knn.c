/**
 * Author: Xun Li <lixun910@gmail.com>
 *
 * Changes:
 * 2021-1-27 Update to use libgeoda 0.0.6
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

#include "weights.h"

#ifndef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif


typedef struct KnnCollectionState
{
    List *geoms;  /* collected geometries */
    List *ogc_fids;
    int k;
    double power;
    bool is_arc;
    bool is_mile;
    Oid geomOid;
} KnnCollectionState;

/**
 * contiguity_context
 *
 * The structure used in function `pg_knn_weights_window` to construct
 * returning results of KNN spatial weights.
 *
 */
typedef struct {
    bool	isdone;
    bool	isnull;
    bytea **result;
    /* variable length */
} knn_context;

/**
 * Window function for SQL `knn_weights()`
 * This will be the main interface for knn weights.
 *
 * @param fcinfo
 * @return
 */
Datum pg_knn_weights_window(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_knn_weights_window);
Datum pg_knn_weights_window(PG_FUNCTION_ARGS) {
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

        // read arguments
        int k = 4;
        if (PG_ARGISNULL(2)) {
            k = DatumGetInt32(WinGetFuncArgCurrent(winobj, 2, &isnull));
            if (isnull || k <= 0) {
                k = 4;
            }
        }

        double power = 1.0;
        if (PG_ARGISNULL(3)) {
            power = DatumGetFloat4(WinGetFuncArgCurrent(winobj, 3, &isnull));
            if (isnull || power < 0) {
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
        PGWeight* w = create_knn_weights(fids, geoms, k, power, is_inverse, is_arc, is_mile);
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

/**
 * bytea_knn_geom_transfn
 *
 * sfunc for aggregate SQL function `geoda_knn_weights()`
 *
 * @param fcinfo
 * @return
 */
Datum bytea_knn_geom_transfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(bytea_knn_geom_transfn);

Datum bytea_knn_geom_transfn(PG_FUNCTION_ARGS)
{
    lwdebug(5, "Enter bytea_knn_geom_transfn().");

    // Get the actual type OID of a specific function argument (counting from 0)
    Datum argType = get_fn_expr_argtype(fcinfo->flinfo, 1);
    if (argType == InvalidOid) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("could not determine input data type")));
    }

    MemoryContext aggcontext;
    if (!AggCheckCallContext(fcinfo, &aggcontext)) {
        elog(ERROR, "bytea_knn_geom_transfn called in non-aggregate context");
        aggcontext = NULL;  /* keep compiler quiet */
    }

    KnnCollectionState* state;
    if ( PG_ARGISNULL(0) ) {
        // first incoming row/item
        state = (KnnCollectionState*)MemoryContextAlloc(aggcontext, sizeof(KnnCollectionState));
        state->geoms = NULL;
        state->ogc_fids = NULL;
        state->k = 4;
        state->power = 1.0;
        state->is_arc = false;
        state->is_mile = false;
        state->geomOid = argType;
        //MemoryContextSwitchTo(old);
    } else {
        state = (KnnCollectionState*) PG_GETARG_POINTER(0);
    }

    LWGEOM* geom;
    int idx;

    /* Take a copy of the geometry into the aggregate context */
    MemoryContext old = MemoryContextSwitchTo(aggcontext);

    int arg_index = 1;

    // fid
    if (!PG_ARGISNULL(arg_index)) {
        idx = PG_GETARG_INT64(arg_index);
    }
    arg_index += 1;

    // the_geom
    if (!PG_ARGISNULL(arg_index)) {
        bytea *bytea_wkb = PG_GETARG_BYTEA_P(arg_index);
        uint8_t *wkb = (uint8_t *) VARDATA(bytea_wkb);
        LWGEOM *lwgeom = lwgeom_from_wkb(wkb, VARSIZE_ANY_EXHDR(bytea_wkb), LW_PARSER_CHECK_ALL);
        geom = lwgeom_clone_deep(lwgeom);
        lwgeom_free(lwgeom);
        //PG_FREE_IF_COPY(bytea_wkb, 0);
    }
    arg_index += 1;

    // k
    int k= 4;
    if (!PG_ARGISNULL(arg_index)) {
        k = PG_GETARG_INT64(arg_index);
    }
    state->k = k;
    arg_index += 1;

    // is_arc
    if (!PG_ARGISNULL(arg_index)) {
        state->is_arc = PG_GETARG_BOOL(arg_index);
    }
    arg_index += 1;

    // is_mile
    if (!PG_ARGISNULL(arg_index)) {
        state->is_mile = PG_GETARG_BOOL(arg_index);
    }
    arg_index += 1;

    /* Initialize or append to list as necessary */
    if (state->geoms) {
        state->geoms = lappend(state->geoms, geom); // pg_list
        state->ogc_fids = lappend_int(state->ogc_fids, idx);
    } else {
        state->geoms = list_make1(geom);
        state->ogc_fids = list_make1_int(idx);
    }

    MemoryContextSwitchTo(old);

    lwdebug(5, "Exit bytea_knn_geom_transfn().");
    PG_RETURN_POINTER(state);
}


Datum geom_knn_weights_bin_finalfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(geom_knn_weights_bin_finalfn);

Datum geom_knn_weights_bin_finalfn(PG_FUNCTION_ARGS)
{
    KnnCollectionState *p;

    lwdebug(1,"Enter geom_knn_weights_bin_finalfn.");

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();   /* returns null iff no input values */

    p = (KnnCollectionState*) PG_GETARG_POINTER(0);

    PGWeight* w = create_knn_weights(p->ogc_fids, p->geoms, p->k, 1.0, false, false, false);

    size_t buf_size = 0;
    uint8_t* w_bytes = weights_to_bytes(w, &buf_size);
    // free PGWeight
    free_pgweight(w);

    /* Prepare the PgSQL text return type */
    bytea *result = palloc(buf_size + VARHDRSZ);
    memcpy(VARDATA(result), w_bytes, buf_size);
    SET_VARSIZE(result, buf_size+VARHDRSZ);

    /* Clean up and return */
    lwfree(w_bytes);
    PG_RETURN_BYTEA_P(result);
}

#ifdef __cplusplus
}
#endif