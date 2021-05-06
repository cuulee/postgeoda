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
    //bytea **result;
    PGWeight *w;
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
    distance_context *context;
    int64 curpos, rowcount;

    rowcount = WinGetPartitionRowCount(winobj);
    context = (distance_context *)WinGetPartitionLocalMemory(winobj, sizeof(distance_context) + sizeof(int) * rowcount);

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
        List *geoms = 0;
        List *fids = 0;

        for (size_t i = 0; i < N; i++) {
            // fid
            Datum arg = WinGetFuncArgInPartition(winobj, 0, i, WINDOW_SEEK_HEAD, false, &isnull, &isout);
            int64 fid = DatumGetInt64(arg);
            //lwdebug(1, "pg_distance_weights_window: %d-th:%d", i, fids[i]);

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

        lwdebug(4, "pg_distance_weights_window: read dist_thres");

        int arg_index = 2;

        // read arguments: dist_thres, power, is_inverse, is_arc, is_mile
        double dist_thres = 0.0;
        if (arg_index < PG_NARGS() ) {
            dist_thres = DatumGetFloat4(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
            if (isnull || dist_thres <= 0) {
                PG_RETURN_NULL();
            }
        }
        arg_index += 1;

        double power = 1.0;
        if (arg_index < PG_NARGS()) {
            power = DatumGetFloat4(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
            if (isnull || power <= 0) {
                power = 1.0;
            }
        }
        arg_index += 1;

        bool is_inverse = false;
        if (arg_index < PG_NARGS() ) {
            is_inverse = DatumGetBool(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
        }
        arg_index += 1;

        bool is_arc = false;
        if (arg_index < PG_NARGS()) {
            is_arc = DatumGetBool(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
        }
        arg_index += 1;

        bool is_mile = false;
        if (arg_index < PG_NARGS()) {
            is_mile = DatumGetBool(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
        }
        arg_index += 1;

        lwdebug(4, "pg_distance_weights_window: create_distance_weights");
        // create weights
        PGWeight* w = create_distance_weights(fids, geoms, dist_thres, power, is_inverse, is_arc, is_mile);
        //bytea **result = weights_to_bytea_array(w);

        // Safe the result
        context->w = w;
        context->isdone = true;

        // free PGWeight
        //free_pgweight(w);

        lwdebug(1, "Exit pg_distance_weights_window. done.");
    }

    if (context->isnull) {
        PG_RETURN_NULL();
    }

    curpos = WinGetCurrentPosition(winobj);
    size_t buf_size = 0;
    buf_size += sizeof(uint32_t); // idx
    uint16_t num_nbrs = context->w->neighbors[curpos].num_nbrs;
    buf_size += sizeof(uint16_t); // num_nbrs
    buf_size += sizeof(uint32_t) * num_nbrs;
    if (context->w->w_type == 'w') {
        buf_size += sizeof(float) * num_nbrs;
    }

    uint8_t *buf = palloc(buf_size);
    uint8_t *pos = buf; // retain the start pos
    memcpy(buf, &(context->w->neighbors[curpos].idx), sizeof(uint32_t)); // copy idx
    buf += sizeof(uint32_t);

    memcpy(buf, &num_nbrs, sizeof(uint16_t)); // copy n_nbrs
    buf += sizeof(uint16_t);

    for (size_t j = 0; j < num_nbrs; ++j) { // copy nbr_id
        memcpy(buf, &context->w->neighbors[curpos].nbrId[j], sizeof(uint32_t));
        buf += sizeof(uint32_t);
    }

    if (context->w->w_type == 'w') {
        for (size_t j = 0; j < num_nbrs; ++j) { // copy nbr_weight
            memcpy(buf, &context->w->neighbors[curpos].nbrWeight[j], sizeof(float));
            buf += sizeof(float);
        }
    }

    // copy to bytea type
    bytea *result = palloc(buf_size + VARHDRSZ);
    SET_VARSIZE(result, buf_size+VARHDRSZ);
    memcpy(VARDATA(result), pos, buf_size);

    // clean
    pfree(pos);

    PG_RETURN_BYTEA_P(result);
}

/**
 * Window function for SQL `kernel_weights()`
 * This will be the main interface for kernel weights.
 *
 * @param fcinfo
 * @return
 */
Datum pg_kernel_weights_window(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_kernel_weights_window);
Datum pg_kernel_weights_window(PG_FUNCTION_ARGS) {
    //this function will be called multiple times until context->isdone==true
    //lwdebug(1, "Enter pg_kernel_weights_window.");

    WindowObject winobj = PG_WINDOW_OBJECT();
    distance_context *context;
    int64 curpos, rowcount;

    rowcount = WinGetPartitionRowCount(winobj);
    context = (distance_context *)WinGetPartitionLocalMemory(winobj, sizeof(distance_context) + sizeof(int) * rowcount);

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
        List *geoms = 0;
        List *fids = 0;

        for (size_t i = 0; i < N; i++) {
            // fid
            Datum arg = WinGetFuncArgInPartition(winobj, 0, i, WINDOW_SEEK_HEAD, false, &isnull, &isout);
            int64 fid = DatumGetInt64(arg);
            //lwdebug(1, "pg_distance_weights_window: %d-th:%d", i, fids[i]);

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

        lwdebug(4, "pg_kernel_weights_window: read dist_thres");

        int arg_index = 2;

        // read arguments: dist_thres, power, is_inverse, is_arc, is_mile
        double dist_thres = 0.0;
        if (arg_index < PG_NARGS() ) {
            dist_thres = DatumGetFloat8(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
            if (isnull || dist_thres <= 0) {
                PG_RETURN_NULL();
            }
        }
        arg_index += 1;

        char *kernel = 0;
        if (arg_index < PG_NARGS()) {
            VarChar *arg = (VarChar *)DatumGetVarCharPP(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
            kernel = (char *)VARDATA(arg);
            lwdebug(1, "Get kernel: %s", kernel);
        }
        if (!check_kernel(kernel)) {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("kernel has to be one of: {triangular, uniform, epanechnikov, quartic, gaussian}")));
        }
        arg_index += 1;

        bool use_kernel_diagonals = false;
        if (arg_index < PG_NARGS()) {
            lwdebug(1, "Get use_kernel_diagonals");
            use_kernel_diagonals = DatumGetBool(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
        }
        arg_index += 1;

        double power = 1.0;
        if (arg_index < PG_NARGS()) {
            power = DatumGetFloat8(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
            if (isnull || power <= 0) {
                power = 1.0;
            }
        }
        arg_index += 1;

        bool is_inverse = false;
        if (arg_index < PG_NARGS() ) {
            is_inverse = DatumGetBool(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
        }
        arg_index += 1;

        bool is_arc = false;
        if (arg_index < PG_NARGS()) {
            is_arc = DatumGetBool(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
        }
        arg_index += 1;

        bool is_mile = false;
        if (arg_index < PG_NARGS()) {
            is_mile = DatumGetBool(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
        }
        arg_index += 1;

        lwdebug(4, "pg_kernel_weights_window: create_distance_weights");
        // create weights
        PGWeight* w = create_kernel_weights(fids, geoms, dist_thres, power, is_inverse, is_arc, is_mile, kernel,
                                            use_kernel_diagonals);
        //bytea **result = weights_to_bytea_array(w);

        // Safe the result
        context->w = w;
        context->isdone = true;

        // free PGWeight
        //free_pgweight(w);

        lwdebug(1, "Exit pg_kernel_weights_window. done.");
    }

    if (context->isnull) {
        PG_RETURN_NULL();
    }

    curpos = WinGetCurrentPosition(winobj);
    size_t buf_size = 0;
    buf_size += sizeof(uint32_t); // idx
    uint16_t num_nbrs = context->w->neighbors[curpos].num_nbrs;
    buf_size += sizeof(uint16_t); // num_nbrs
    buf_size += sizeof(uint32_t) * num_nbrs;
    if (context->w->w_type == 'w') {
        buf_size += sizeof(float) * num_nbrs;
    }

    uint8_t *buf = palloc(buf_size);
    uint8_t *pos = buf; // retain the start pos
    memcpy(buf, &(context->w->neighbors[curpos].idx), sizeof(uint32_t)); // copy idx
    buf += sizeof(uint32_t);

    memcpy(buf, &num_nbrs, sizeof(uint16_t)); // copy n_nbrs
    buf += sizeof(uint16_t);

    for (size_t j = 0; j < num_nbrs; ++j) { // copy nbr_id
        memcpy(buf, &context->w->neighbors[curpos].nbrId[j], sizeof(uint32_t));
        buf += sizeof(uint32_t);
    }

    if (context->w->w_type == 'w') {
        for (size_t j = 0; j < num_nbrs; ++j) { // copy nbr_weight
            memcpy(buf, &context->w->neighbors[curpos].nbrWeight[j], sizeof(float));
            buf += sizeof(float);
        }
    }

    // copy to bytea type
    bytea *result = palloc(buf_size + VARHDRSZ);
    SET_VARSIZE(result, buf_size+VARHDRSZ);
    memcpy(VARDATA(result), pos, buf_size);

    // clean
    pfree(pos);

    PG_RETURN_BYTEA_P(result);
}

/**
 *  WeightsCollectionState
 *
 *  This is used for collecting geometries in an Aggregate function.
 */
typedef struct
{
    List *geoms;  /* collected geometries */
    List *fids;
    bool is_arc;
    bool is_mile;
    Oid geomOid;
} WeightsCollectionState;

/**
 * bytea_to_geom_dist_transfn()
 *
 * This is for the Aggregate function that collects all geometries and calculate the
 * minimum pairwise distance.
 *
 * @param fcinfo
 * @return
 */
Datum bytea_to_geom_dist_transfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(bytea_to_geom_dist_transfn);
Datum bytea_to_geom_dist_transfn(PG_FUNCTION_ARGS)
{
    lwdebug(4, "Enter bytea_to_geom_dist_transfn().");

    // Get the actual type OID of a specific function argument (counting from 0)
    Datum argType = get_fn_expr_argtype(fcinfo->flinfo, 1);
    if (argType == InvalidOid) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("could not determine input data type")));
    }

    MemoryContext aggcontext;
    if (!AggCheckCallContext(fcinfo, &aggcontext)) {
        elog(ERROR, "bytea_to_geom_dist_transfn called in non-aggregate context");
        aggcontext = NULL;  /* keep compiler quiet */
    }

    WeightsCollectionState* state;
    if ( PG_ARGISNULL(0) ) {
        // first incoming row/item
        state = (WeightsCollectionState*)MemoryContextAlloc(aggcontext, sizeof(WeightsCollectionState));
        state->geoms = NULL;
        state->fids = NULL;
        state->is_mile = false;
        state->is_arc = false;
        state->geomOid = argType;
        //MemoryContextSwitchTo(old);
    } else {
        state = (WeightsCollectionState*) PG_GETARG_POINTER(0);
    }

    LWGEOM* geom = 0;
    int idx = 0;

    // Take a copy of the geometry into the aggregate context
    MemoryContext old = MemoryContextSwitchTo(aggcontext);

    int arg_index = 1;

    // fid
    if (!PG_ARGISNULL(arg_index)) {
        idx = PG_GETARG_INT64(arg_index);
    }
    arg_index += 1;

    // the_geom
    if (!PG_ARGISNULL(arg_index)) {
        bytea *bytea_wkb = (bytea*)PG_DETOAST_DATUM_COPY(PG_GETARG_DATUM(arg_index));
        uint8_t *wkb = (uint8_t *) VARDATA(bytea_wkb);
        geom = lwgeom_from_wkb(wkb, VARSIZE_ANY_EXHDR(bytea_wkb), LW_PARSER_CHECK_ALL);
        //PG_FREE_IF_COPY(bytea_wkb, 0);
    }
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

    // Initialize or append to list as necessary
    if (state->geoms) {
        lwdebug(4, "lappend geom.");
        state->geoms = lappend(state->geoms, geom); // pg_list
        state->fids = lappend_int(state->fids, idx);
    } else {
        lwdebug(4, "list_make.");
        state->geoms = list_make1(geom);
        state->fids = list_make1_int(idx);
    }

    MemoryContextSwitchTo(old);

    PG_RETURN_POINTER(state);
}
/**
 * geom_to_dist_threshold_finalfn()
 *
 * This is the finalfunc for min_distthreshold() SQL function.
 *
 * @param fcinfo
 * @return
 */
Datum geom_to_dist_threshold_finalfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(geom_to_dist_threshold_finalfn);
Datum geom_to_dist_threshold_finalfn(PG_FUNCTION_ARGS)
{
    WeightsCollectionState *p;

    lwdebug(1,"Enter geom_to_dist_threshold_finalfn.");

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();   /* returns null iff no input values */
    }

    // get State from aggregate internal function
    p = (WeightsCollectionState*) PG_GETARG_POINTER(0);

    double dist = get_min_distthreshold(p->fids, p->geoms, p->is_arc, p->is_mile);

    lwdebug(1,"Exit geom_to_dist_threshold_finalfn.");
    PG_RETURN_FLOAT4(dist);
}


#ifdef __cplusplus
}
#endif