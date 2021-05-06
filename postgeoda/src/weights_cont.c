/**
 * Author: Xun Li <lixun910@gmail.com>
 *
 * Changes:
 * 2021-1-27 Update to use libgeoda 0.0.6
 * 2021-4-23 Add contiguity_context, pg_queen_weights_window()
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

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif


/**
 * contiguity_context
 *
 * The structure used in function `pg_queen_weights_window` to construct
 * returning results of contiguity spatial weights.
 *
 */
typedef struct {
    bool	isdone;
    bool	isnull;
    bytea   **result;
    PGWeight *w;
    /* variable length */
} contiguity_context;

/**
 * Window function for SQL `queen_weights()`
 * This will be the main interface for Queen weights
 *
 * @param fcinfo
 * @return
 */
Datum pg_queen_weights_window(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_queen_weights_window);
Datum pg_queen_weights_window(PG_FUNCTION_ARGS) {
    //this function will be called multiple times until context->isdone==true

    WindowObject winobj = PG_WINDOW_OBJECT();
    contiguity_context *context;
    int64 curpos, rowcount;

    rowcount = WinGetPartitionRowCount(winobj);
    context = (contiguity_context *) WinGetPartitionLocalMemory(winobj, sizeof(contiguity_context) + sizeof(int) * rowcount);

    if (!context->isdone) {
        bool isnull, isout;

        /* We also need a non-zero N */
        int N = (int) WinGetPartitionRowCount(winobj);
        lwdebug(1,"Enter pg_queen_weights_window: N=%d", N);
        if (N <= 0) {
            context->isdone = true;
            context->isnull = true;
            PG_RETURN_NULL();
        }

        // Read all the geometries from the partition window into a list
        List *geoms = 0;
        List *fids = 0;

        for (size_t i = 0; i < N; i++) {
            // fid
            Datum arg = WinGetFuncArgInPartition(winobj, 0, i, WINDOW_SEEK_HEAD, false, &isnull, &isout);
            int64 fid = DatumGetInt64(arg);

            // the_geom
            Datum arg1 = WinGetFuncArgInPartition(winobj, 1, i, WINDOW_SEEK_HEAD, false, &isnull, &isout);
            bytea *bytea_wkb = (bytea*)PG_DETOAST_DATUM_COPY(arg1);
            uint8_t *wkb = (uint8_t*)VARDATA(bytea_wkb);
            LWGEOM *lwgeom = lwgeom_from_wkb(wkb, VARSIZE_ANY_EXHDR(bytea_wkb), LW_PARSER_CHECK_ALL);
            if (i==0) {
                geoms = list_make1(lwgeom);
                fids = list_make1_int(fid);
            } else {
                geoms = lappend(geoms, lwgeom);
                fids = lappend_int(fids, fid);
            }
        }

        int arg_index = 2;

        // bool is_queen, int order, bool inc_lower, double precision_threshold
        int order = 1;
        if (arg_index < PG_NARGS() ) {
            order = DatumGetInt32(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
            if (isnull || order <= 0) {
                order = 1;
            }
        }
        arg_index +=1;

        bool inc_lower = false;
        if (arg_index < PG_NARGS() ) {
            inc_lower = DatumGetBool(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
        }
        arg_index +=1;

        double precision_threshold = 0.0;
        if (arg_index < PG_NARGS()) {
            precision_threshold = DatumGetFloat4(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
            if (isnull || precision_threshold < 0) {
                precision_threshold = 0.0;
            }
        }
        arg_index +=1;

        // create weights
        PGWeight* w = create_cont_weights(fids, geoms, true, order, inc_lower, precision_threshold);
        //bytea **result = weights_to_bytea_array(w);

        // Safe the result
        context->w = w;
        context->isdone = true;

        // free PGWeight
        //free_pgweight(w);

        lwdebug(1, "Exit pg_queen_weights_window. done.");
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
 * Window function for SQL `rook_weights()`
 * This will be the main interface for Rook weights
 *
 * @param fcinfo
 * @return
 */
Datum pg_rook_weights_window(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_rook_weights_window);
Datum pg_rook_weights_window(PG_FUNCTION_ARGS) {
    //this function will be called multiple times until context->isdone==true

    WindowObject winobj = PG_WINDOW_OBJECT();
    contiguity_context *context;
    int64 curpos, rowcount;

    rowcount = WinGetPartitionRowCount(winobj);
    context = (contiguity_context *) WinGetPartitionLocalMemory(winobj, sizeof(contiguity_context) + sizeof(int) * rowcount);

    if (!context->isdone) {
        bool isnull, isout;

        /* We also need a non-zero N */
        int N = (int) WinGetPartitionRowCount(winobj);
        lwdebug(1,"Enter pg_rook_weights_window: N=%d", N);
        if (N <= 0) {
            context->isdone = true;
            context->isnull = true;
            PG_RETURN_NULL();
        }

        // Read all the geometries from the partition window into a list
        List *geoms = 0;
        List *fids = 0;

        for (size_t i = 0; i < N; i++) {
            // fid
            Datum arg = WinGetFuncArgInPartition(winobj, 0, i, WINDOW_SEEK_HEAD, false, &isnull, &isout);
            int64 fid = DatumGetInt64(arg);

            // the_geom
            Datum arg1 = WinGetFuncArgInPartition(winobj, 1, i, WINDOW_SEEK_HEAD, false, &isnull, &isout);
            bytea *bytea_wkb = (bytea*)PG_DETOAST_DATUM_COPY(arg1);
            uint8_t *wkb = (uint8_t*)VARDATA(bytea_wkb);
            LWGEOM *lwgeom = lwgeom_from_wkb(wkb, VARSIZE_ANY_EXHDR(bytea_wkb), LW_PARSER_CHECK_ALL);
            if (i==0) {
                geoms = list_make1(lwgeom);
                fids = list_make1_int(fid);
            } else {
                geoms = lappend(geoms, lwgeom);
                fids = lappend_int(fids, fid);
            }
        }

        int arg_index = 2;

        // bool is_queen, int order, bool inc_lower, double precision_threshold
        int order = 1;
        if (arg_index < PG_NARGS()) {
            order = DatumGetInt32(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
            if (isnull || order <= 0) {
                order = 1;
            }
        }
        arg_index += 1;

        bool inc_lower = false;
        if (arg_index < PG_NARGS()) {
            inc_lower = DatumGetBool(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
        }
        arg_index += 1;

        double precision_threshold = 0.0;
        if (arg_index < PG_NARGS()) {
            precision_threshold = DatumGetFloat4(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
            if (isnull || precision_threshold < 0) {
                precision_threshold = 0.0;
            }
        }
        arg_index += 1;

        // create weights
        PGWeight* w = create_cont_weights(fids, geoms, false, order, inc_lower, precision_threshold);
        //bytea **result = weights_to_bytea_array(w);

        // Safe the result
        context->w = w;
        context->isdone = true;

        // free PGWeight
        //free_pgweight(w);

        lwdebug(1, "Exit pg_rook_weights_window. done.");
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
 * weights_to_text
 *
 * Used in NORAML SQL query: GEODA_WEIGHTS_ASTEXT(bytea)
 *
 * The input is a bytea, which represents weights information for
 * one observation in one row
 *
 * BINARY format:
 * uint32 (4 bytes): index of i-th observation
 * uint16 (2 bytes): number of neighbors of i-th observation (nn)
 * uint32 (4 bytes x nn): neighbor id
 * float (4 bytes x nn): weights value of each neighbor
 *
 * @param fcinfo
 * @return
 */
Datum weights_to_text(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(weights_to_text);
Datum weights_to_text(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();   /* returns null iff no input values */
    }

    bytea *bytea_w = PG_GETARG_BYTEA_P(0);
    uint8_t *bw = (uint8_t *) VARDATA(bytea_w);
    size_t bw_size = VARSIZE_ANY_EXHDR(bytea_w);

    uint32_t fid;
    uint16_t n_nbrs;

    uint8_t *pos = bw;

    // idx
    memcpy(&fid, pos, sizeof(uint32_t));
    pos += sizeof(uint32_t);

    lwdebug(4,"Enter weights_to_text(). fid=%d", fid);

    // number of neighbors
    memcpy(&n_nbrs, pos, sizeof(uint16_t));
    pos += sizeof(uint16_t);

    char *c_nbrs = lwalloc(BUFSIZE * n_nbrs * sizeof(char));
    snprintf(c_nbrs, BUFSIZE, "%d:", fid);

    // check if has weight val (wval)
    bool has_wval = bw_size > (size_t)(pos + sizeof(uint32_t) * n_nbrs - bw);

    if (has_wval) {
        strcat(c_nbrs, "[");
    }
    strcat(c_nbrs, "[");

    // read neighbor id
    uint32_t n_id;
    for (size_t j=0; j<n_nbrs; ++j)  {
        char buffer[BUFSIZE];
        memcpy(&n_id, pos, sizeof(uint32_t));
        pos += sizeof(uint32_t);

        snprintf(buffer, BUFSIZE, "%d", n_id);
        strcat(c_nbrs, buffer);
        if (j < n_nbrs-1)
            strcat(c_nbrs, ",");
    }

    strcat(c_nbrs, "]");

    // read neighbor weights
    if (has_wval) {
        strcat(c_nbrs, ",[");
        float weight;
        char buffer[BUFSIZE];
        for (size_t j=0; j<n_nbrs; ++j)  {
            memcpy(&weight, pos, sizeof(float));
            pos += sizeof(float);

            snprintf(buffer, BUFSIZE, "%f", weight);
            strcat(c_nbrs, buffer);
            if (j < n_nbrs - 1)
                strcat(c_nbrs, ",");
        }
        strcat(c_nbrs, "]]");
    }

    // prepare result
    text *result;
    size_t len = strlen(c_nbrs);

    size_t res_len = len + VARHDRSZ;
    result = (text*)palloc(res_len);
    SET_VARSIZE(result, res_len);
    memcpy(VARDATA(result), c_nbrs, len);

    lwfree(c_nbrs);

    lwdebug(4,"Exit weights_to_text(). fid=%d", fid);

    PG_RETURN_TEXT_P(result);
}

// Following lines of code are for AGGREGATE SQL functions

typedef struct WeightsAccessState
{
    char w_type;
    uint8_t *w;
    int ctx_current;
    int ctx_count;
} WeightsAccessState;

typedef struct CollectionBuildState
{
    List *geoms;  /* collected geometries */
    List *ogc_fids;
    bool is_queen;
    int order;
    bool inc_lower;
    double precision_threshold;
    Oid geomOid;
} CollectionBuildState;



typedef struct WeightsAccessContext
{
    char w_type;
    uint32_t num_obs;
    uint8_t *pos;
} WeightsAccessContext;

/**
 * weights_bytea_to_set
 *
 * Covert the weights bytea to a set of table rows
 *
 * @param fcinfo
 * @return
 */
Datum weights_bytea_to_set(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(weights_bytea_to_set);
Datum weights_bytea_to_set(PG_FUNCTION_ARGS)
{
    FuncCallContext     *funcctx;
    int                  call_cntr;
    int                  max_calls;

    // stuff done only on the first call of the function
    if (SRF_IS_FIRSTCALL()) {
        // save current memory context
        MemoryContext   oldcontext;
        // create a function context for cross-call persistence
        funcctx = SRF_FIRSTCALL_INIT();
        // switch to memory context appropriate for multiple function calls
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        // create user memory context
        WeightsAccessContext *w_fct = palloc(sizeof(WeightsAccessContext));
        funcctx->user_fctx = w_fct;

        bytea *bw = PG_GETARG_BYTEA_P(0);
        uint8_t *pos = (uint8_t*)VARDATA(bw);

        memcpy(&w_fct->w_type, pos, sizeof(char));
        pos += sizeof(char);

        memcpy(&w_fct->num_obs, pos, sizeof(uint32_t));
        pos += sizeof(uint32_t); // skip num_obs

        w_fct->pos  = pos;

        // total number of tuples to be returned
        funcctx->max_calls = w_fct->num_obs;

        MemoryContextSwitchTo(oldcontext);
    }

    // stuff done on every call of the function
    funcctx = SRF_PERCALL_SETUP();
    call_cntr = funcctx->call_cntr;
    max_calls = funcctx->max_calls;

    WeightsAccessContext *w_fct = (WeightsAccessContext*)funcctx->user_fctx;
    uint16_t n_nbrs;

    //  do when there is more left to send
    if (call_cntr < max_calls) {
        // read for every observation
        uint8_t *pos = w_fct->pos;
        // read idx
        pos += sizeof(uint32_t);
        // read number of neighbors
        memcpy(&n_nbrs, pos, sizeof(uint16_t));
        pos += sizeof(uint16_t);
        // read neighbor idx and weights
        for (size_t j=0; j<n_nbrs; ++j)  {
            pos += sizeof(uint32_t);
        }
        if (w_fct->w_type == 'w') {
            for (size_t j=0; j<n_nbrs; ++j)  {
                // read neighbor weight
                pos += sizeof(float);
            }
        }

        size_t buf_size = pos - w_fct->pos;
        bytea *result = palloc(buf_size + VARHDRSZ);
        memcpy(VARDATA(result), w_fct->pos, buf_size);
        SET_VARSIZE(result, buf_size+VARHDRSZ);

        // move pointer to next observation
        w_fct->pos = pos;

        SRF_RETURN_NEXT(funcctx, PointerGetDatum(result));
    } else {
        // do when there is no more left
        SRF_RETURN_DONE(funcctx);
    }
}

/**
 * weights_bytea_getfids
 *
 * Get fids from the weights bytea, and return them as  a set of table rows
 *
 * @param fcinfo
 * @return
 */
Datum weights_bytea_getfids(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(weights_bytea_getfids);
Datum weights_bytea_getfids(PG_FUNCTION_ARGS)
{
    FuncCallContext     *funcctx;
    int                  call_cntr;
    int                  max_calls;

    // stuff done only on the first call of the function
    if (SRF_IS_FIRSTCALL()) {
        // save current memory context
        MemoryContext   oldcontext;
        // create a function context for cross-call persistence
        funcctx = SRF_FIRSTCALL_INIT();
        // switch to memory context appropriate for multiple function calls
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        // create user memory context
        WeightsAccessContext *w_fct = palloc(sizeof(WeightsAccessContext));
        funcctx->user_fctx = w_fct;

        bytea *bw = PG_GETARG_BYTEA_P(0);
        uint8_t *pos = (uint8_t*)VARDATA(bw);

        memcpy(&w_fct->w_type, pos, sizeof(char));
        pos += sizeof(char);

        memcpy(&w_fct->num_obs, pos, sizeof(uint32_t));
        pos += sizeof(uint32_t); // skip num_obs

        w_fct->pos  = pos;

        // total number of tuples to be returned
        funcctx->max_calls = w_fct->num_obs;

        MemoryContextSwitchTo(oldcontext);
    }

    // stuff done on every call of the function
    funcctx = SRF_PERCALL_SETUP();
    call_cntr = funcctx->call_cntr;
    max_calls = funcctx->max_calls;

    WeightsAccessContext *w_fct = (WeightsAccessContext*)funcctx->user_fctx;
    uint32_t fid;
    uint16_t n_nbrs;

    //  do when there is more left to send
    if (call_cntr < max_calls) {
        // read for every observation
        uint8_t *pos = w_fct->pos;
        // read idx
        memcpy(&fid, pos, sizeof(uint16_t));
        pos += sizeof(uint32_t);

        // read number of neighbors
        memcpy(&n_nbrs, pos, sizeof(uint16_t));
        pos += sizeof(uint16_t);
        // read neighbor idx and weights
        for (size_t j=0; j<n_nbrs; ++j)  {
            pos += sizeof(uint32_t);
        }
        if (w_fct->w_type == 'w') {
            for (size_t j=0; j<n_nbrs; ++j)  {
                // read neighbor weight
                pos += sizeof(float);
            }
        }

        // move pointer to next observation
        w_fct->pos = pos;

        SRF_RETURN_NEXT(funcctx, Int64GetDatum(fid));
    } else {
        // do when there is no more left
        SRF_RETURN_DONE(funcctx);
    }
}

/**
 * bytea_to_geom_transfn
 *
 * This is an Internal `sfunc` function, which is used to collect all geometries
 * and fids from SELECT query. The collected information will be shared using
 * MemoryContext.
 *
 * @param fcinfo
 * @return Pointer to CollectionBuildState
 */
Datum bytea_to_geom_transfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(bytea_to_geom_transfn);

Datum bytea_to_geom_transfn(PG_FUNCTION_ARGS)
{
    lwdebug(4, "Enter bytea_to_geom_transfn().");

    // Get the actual type OID of a specific function argument (counting from 0)
    Datum argType = get_fn_expr_argtype(fcinfo->flinfo, 1);
    if (argType == InvalidOid) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("could not determine input data type")));
    }
    
    MemoryContext aggcontext;
    if (!AggCheckCallContext(fcinfo, &aggcontext)) {
        elog(ERROR, "bytea_to_geom_transfn called in non-aggregate context");
        aggcontext = NULL;  /* keep compiler quiet */
    }

    CollectionBuildState* state;
    if ( PG_ARGISNULL(0) ) {
        // first incoming row/item
		state = (CollectionBuildState*)MemoryContextAlloc(aggcontext, sizeof(CollectionBuildState));
		state->geoms = NULL;
		state->ogc_fids = NULL;
		state->is_queen = true;
		state->order = 1;
		state->inc_lower = false;
		state->precision_threshold = 0;
		state->geomOid = argType;
        //MemoryContextSwitchTo(old);
	} else {
		state = (CollectionBuildState*) PG_GETARG_POINTER(0);
	}

    LWGEOM* geom = 0;
    int idx = 0;

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

    // is_queen
    if (!PG_ARGISNULL(arg_index)) {
        state->inc_lower = PG_GETARG_BOOL(arg_index);
    }
    arg_index += 1;

    // order_of_contiguity
    if (!PG_ARGISNULL(arg_index)) {
        state->order = PG_GETARG_INT32(arg_index);
    }
    arg_index += 1;

    // include_lower_order
    if (!PG_ARGISNULL(arg_index)) {
        state->inc_lower = PG_GETARG_BOOL(arg_index);
    }
    arg_index += 1;

    // precision_threshold
    if (!PG_ARGISNULL(arg_index)) {
        state->precision_threshold = PG_GETARG_FLOAT4(arg_index);
    }
    arg_index += 1;

    /* Initialize or append to list as necessary */
    if (state->geoms) {
        lwdebug(4, "lappend geom.");
        state->geoms = lappend(state->geoms, geom); // pg_list
        state->ogc_fids = lappend_int(state->ogc_fids, idx);
    } else {
        lwdebug(4, "list_make.");
        state->geoms = list_make1(geom);
        state->ogc_fids = list_make1_int(idx);
    }

    MemoryContextSwitchTo(old);

    PG_RETURN_POINTER(state);
}

/**
 * weights_bytea_tojson
 *
 * Used in sql GEODA_WEIGHTS_TOJSON
 * Input parameter is the bytea of a complete weights
 *
 * @param fcinfo
 * @return text
 */
Datum weights_bytea_tojson(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(weights_bytea_tojson);

Datum weights_bytea_tojson(PG_FUNCTION_ARGS)
{
    lwdebug(1,"Enter new weights_bytea_tojson.");

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();   /* returns null iff no input values */

    bytea *bw = PG_GETARG_BYTEA_P(0);

    uint8_t *pos = (uint8_t*)VARDATA(bw);

    uint32_t idx = 0;
    uint16_t n_nbrs = 0;
    char w_type;

    memcpy(&w_type, pos, sizeof(char));
    pos += sizeof(char); // weights type char

    uint32_t num_obs = 0;
    memcpy(&num_obs, pos, sizeof(uint32_t));
    pos += sizeof(uint32_t); // num_obs

    lwdebug(5, "num_obs=%d", num_obs);

    List *c_arr = 0;
    size_t n_arr = 0, i, j;
    uint32_t nid;
    float nweights;
    char *c_nbrs;

    for (i = 0; i < num_obs; ++i) {
        // read idx
        memcpy(&idx, pos, sizeof(uint32_t));
        pos += sizeof(uint32_t);

        // read number of neighbors
        memcpy(&n_nbrs, pos, sizeof(uint16_t));
        pos += sizeof(uint16_t);

        // create string memory
        c_nbrs = lwalloc(BUFSIZE * n_nbrs * sizeof(char));

        // assign string content
        snprintf(c_nbrs, BUFSIZE, "%d:", idx);

        if (w_type == 'w') {
            strcat(c_nbrs, "[");
        }
        strcat(c_nbrs, "[");

        // loop neighbrs
        for (j = 0; j < n_nbrs; ++j) {
            char buffer[BUFSIZE];
            // read neighbor id
            memcpy(&nid, pos, sizeof(uint32_t));
            pos += sizeof(uint32_t);

            snprintf(buffer, BUFSIZE, "%d", nid);

            strcat(c_nbrs, buffer);
            if (j < n_nbrs - 1)
                strcat(c_nbrs, ",");
        }

        strcat(c_nbrs, "]");

        if (w_type == 'w') {
            strcat(c_nbrs, ",[");
            for (j = 0; j < n_nbrs; ++j) {
                char buffer[BUFSIZE];
                // read neighbor weight
                memcpy(&nweights, pos, sizeof(float));
                pos += sizeof(float);

                snprintf(buffer, BUFSIZE, "%f", nweights);
                strcat(c_nbrs, buffer);
                if (j < n_nbrs - 1)
                    strcat(c_nbrs, ",");
            }
            strcat(c_nbrs, "]]");
        }

        if (i < num_obs - 1)
            strcat(c_nbrs, ",");

        n_arr += strlen(c_nbrs);

        if (i == 0) {
            c_arr = list_make1(c_nbrs);
        } else {
            c_arr = lappend(c_arr, c_nbrs);
        }

        //lwdebug(1, "text=%s length=%d", c_nbrs, n_arr);
    }

    // compose returned json string
    size_t len = n_arr + 2;
    char *w_str = palloc(len * sizeof(char));
    char *o_w_str = w_str;
    strcpy(w_str, "{");
    w_str += 1;

    ListCell *l;
    foreach (l, c_arr) {
        char *c_nbrs = (char *) (lfirst(l));
        strcpy(w_str, c_nbrs);
        //lwdebug(1, "text=%s", o_w_str);
        w_str += strlen(c_nbrs);
        lwfree(c_nbrs);
    }
    strcpy(w_str, "}");

    text *result;
    //lwdebug(1, "new text=%s, total length=%d", o_w_str, len);

    size_t res_len = len + VARHDRSZ;

    result = (text *) palloc(res_len);
    SET_VARSIZE(result, res_len);
    memcpy(VARDATA(result), o_w_str, len);

    pfree(o_w_str);

    lwdebug(1,"Exit weights_bytea_tojson.");
    PG_RETURN_TEXT_P(result);
}

/**
 * Internal function
 *
 * geom_to_weights
 *
 * Used in AGGREGATE SQL function: GEODA_WEIGHTS_QUEEN
 *
 * Collect the select geoms and fids, and create a Queen weights
 * ,which is then returned as bytea
 *
 * @param fcinfo
 * @return bytea
 */
Datum geom_to_contweights_finalfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(geom_to_contweights_finalfn);

Datum geom_to_contweights_finalfn(PG_FUNCTION_ARGS)
{
    CollectionBuildState *p;

    lwdebug(1,"Enter geom_to_contweights_finalfn.");

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();   /* returns null iff no input values */
    }

    // get State from aggregate internal function
    p = (CollectionBuildState*) PG_GETARG_POINTER(0);

    PGWeight* w = create_cont_weights(p->ogc_fids, p->geoms, p->is_queen, p->order, p->inc_lower,
            p->precision_threshold);

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

    lwdebug(1,"Exit geom_to_contweights_finalfn.");
    PG_RETURN_BYTEA_P(result);
}

/**
 * Deprecated
 *
 * @param fcinfo
 * @return
 */
Datum PGWeight_at(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(PGWeight_at);
Datum PGWeight_at(PG_FUNCTION_ARGS)
{
    int64 fid = PG_GETARG_INT64(0);

    bytea *bw = PG_GETARG_BYTEA_P(1);
    uint8_t *w = (uint8_t*)VARDATA(bw);

    const uint8_t *pos = w;

    uint32_t idx = 0;
    uint16_t n_nbrs = 0;

    char w_type;
    memcpy(&w_type, pos, sizeof(char));
    pos += sizeof(char); // skip weights type char

    uint32_t num_obs = 0;
    memcpy(&num_obs, pos, sizeof(uint32_t));
    pos += sizeof(uint32_t); // skip num_obs

    const uint8_t *start = pos;
    size_t buf_size = 0;

    for (size_t i=0; i<num_obs; ++i)  {
        // read idx
        memcpy(&idx, pos, sizeof(uint32_t));
        if (idx == fid) {
            start = pos;
        }
        pos += sizeof(uint32_t);

        // read number of neighbors
        memcpy(&n_nbrs, pos, sizeof(uint16_t));
        pos += sizeof(uint16_t);
        // read neighbor idx and weights
        for (size_t j=0; j<n_nbrs; ++j)  {
            pos += sizeof(uint32_t);
        }
        if (w_type == 'w') {
            for (size_t j=0; j<n_nbrs; ++j)  {
                // read neighbor weight
                pos += sizeof(float);
            }
        }
        if (idx == fid) {
            buf_size = pos - start;
            break;
        }
    }
    if (buf_size == 0)
        PG_RETURN_NULL();

    bytea *result = palloc(buf_size + VARHDRSZ);
    memcpy(VARDATA(result), start, buf_size);
    SET_VARSIZE(result, buf_size+VARHDRSZ);

    PG_RETURN_BYTEA_P(result);
}

/**
 * DEPRECATED
 *
 * MemoryCoontextAlloc and cache (fn_extra) is used
 *
 * PGWEight_to_set
 * @param fcinfo
 * @return
 */
Datum PGWeight_to_set(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(PGWeight_to_set);
Datum PGWeight_to_set(PG_FUNCTION_ARGS)
{
    bytea *bw = PG_GETARG_BYTEA_P(0);
    uint8_t *w = (uint8_t*)VARDATA(bw);

    FmgrInfo      * fmgr_info  = fcinfo->flinfo;
    ReturnSetInfo * resultInfo = (ReturnSetInfo *)fcinfo->resultinfo;
    WeightsAccessState *ctx;

    if( fcinfo->resultinfo == NULL )
        elog(ERROR, "weights_queen: context does not accept a set result");

    if( !IsA( fcinfo->resultinfo, ReturnSetInfo ))
        elog(ERROR, "weights_queen: context does not accept a set result");

    if( fmgr_info->fn_extra == NULL ) {
        fmgr_info->fn_extra = MemoryContextAlloc( fmgr_info->fn_mcxt,
                                                  sizeof(WeightsAccessState));

        WeightsAccessState* new_ctx = (WeightsAccessState *)fmgr_info->fn_extra;

        uint8_t *pos = w; // start position
        char w_type;
        memcpy(&w_type, pos, sizeof(char));
        new_ctx->w_type = w_type;
        pos += sizeof(char); // skip weights type char

        uint32_t num_obs = 0;
        memcpy(&num_obs, pos, sizeof(uint32_t));
        pos += sizeof(uint32_t); // skip num_obs

        new_ctx->ctx_count = num_obs;
        new_ctx->ctx_current = 0;
        new_ctx->w = pos;
    }

    ctx = (WeightsAccessState *)fmgr_info->fn_extra;
    if( ctx->ctx_count == -1 ) {
        pfree( fmgr_info->fn_extra );
        fmgr_info->fn_extra = NULL;
        resultInfo->isDone = ExprEndResult;
        PG_RETURN_NULL();
    }

    if( ctx->ctx_current < ctx->ctx_count ) {
        uint8_t *pos = ctx->w;

        //uint32_t idx = 0;
        uint16_t n_nbrs = 0;
        //uint32_t n_id=0;

        const uint8_t *start = pos;

        //for (size_t i=0; i<ctx->ctx_current; ++i)  {
        //    if (i == ctx->ctx_current -1)
        //        start = pos;

        // read idx
        pos += sizeof(uint32_t);
        // read number of neighbors
        memcpy(&n_nbrs, pos, sizeof(uint16_t));
        pos += sizeof(uint16_t);
        // read neighbor idx and weights
        for (size_t j=0; j<n_nbrs; ++j)  {
            pos += sizeof(uint32_t);
        }
        if (ctx->w_type == 'w') {
            for (size_t j=0; j<n_nbrs; ++j)  {
                // read neighbor weight
                pos += sizeof(float);
            }
        }
        //}

        // create results
        size_t buf_size = pos - start;
        bytea *result = palloc(buf_size + VARHDRSZ);
        memcpy(VARDATA(result), start, buf_size);
        SET_VARSIZE(result, buf_size+VARHDRSZ);

        resultInfo->isDone = ExprMultipleResult;
        // Advance to the next entry in our array of
        ctx->ctx_current++;
        ctx->w = pos;

        PG_RETURN_BYTEA_P( result );
    } else {
        pfree( fmgr_info->fn_extra );
        fmgr_info->fn_extra = NULL;
        resultInfo->isDone = ExprEndResult;
        PG_RETURN_NULL();
    }
    PG_RETURN_BYTEA_P(NULL);
}

///////////////////////////////////////////////////////////////////////////////////////////
// test
PG_FUNCTION_INFO_V1(grt_sfunc);


Datum
grt_sfunc(PG_FUNCTION_ARGS)
{
    Point *new_agg_state = (Point *) palloc(sizeof(Point));
    double el = PG_GETARG_FLOAT8(1);
    bool isnull = PG_ARGISNULL(0);
    if(isnull) {
        new_agg_state->x = el;
        new_agg_state->y = el;
        PG_RETURN_POINTER(new_agg_state);
    }
    Point *agg_state = (Point*)PG_GETARG_POINTER(0);

    new_agg_state->x = agg_state->x + el;
    if(new_agg_state->x > agg_state->y) {
        new_agg_state->y = new_agg_state->x;
    } else {
        new_agg_state->y = agg_state->y;
    }

    PG_RETURN_POINTER(new_agg_state);
}

/*
uint32_t array_nelems_not_null(ArrayType* array) {
    ArrayIterator iterator;
    Datum value;
    bool isnull;
    uint32_t nelems_not_null = 0;
    iterator = array_create_iterator(array, 0, NULL);
    while(array_iterate(iterator, &value, &isnull) )
        if (!isnull)
            nelems_not_null++;

    array_free_iterator(iterator);

    return nelems_not_null;
}
 */

Datum pg_all_queries(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_all_queries);

Datum pg_all_queries(PG_FUNCTION_ARGS)
{
    return (Datum) 0;
}

/**
* A modified version of PostgreSQL's DirectFunctionCall1 which allows NULL results; this
* is required for aggregates that return NULL.
*/
Datum PGISDirectFunctionCall1(PGFunction func, Datum arg1);
Datum
PGISDirectFunctionCall1(PGFunction func, Datum arg1)
{
    Datum result = 0;

#if POSTGIS_PGSQL_VERSION < 120
    /*
    FunctionCallInfoData fcinfo;
    InitFunctionCallInfoData(fcinfo, NULL, 1, InvalidOid, NULL, NULL);
    fcinfo.arg[0] = arg1;
    fcinfo.argnull[0] = false;

    result = (*func) (&fcinfo);

    // check for null result, returning a "NULL" Datum if indicated
    if (fcinfo.isnull) {
        return (Datum) 0;
    }
     */
#else
    LOCAL_FCINFO(fcinfo, 1);

	InitFunctionCallInfoData(*fcinfo, NULL, 1, InvalidOid, NULL, NULL);

	fcinfo->args[0].value = arg1;
	fcinfo->args[0].isnull = false;

	result = (*func)(fcinfo);

	/* Check for null result, returning a "NULL" Datum if indicated */
	if (fcinfo->isnull) {
		return (Datum)0;
	}

#endif /* POSTGIS_PGSQL_VERSION < 120 */
    return result;
}


#ifdef __cplusplus
}
#endif

