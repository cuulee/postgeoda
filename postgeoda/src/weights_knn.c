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


#include "config.h"
#include "geoms.h"
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
    Oid geomOid;
} KnnCollectionState;

Datum bytea_knn_geom_transfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(bytea_knn_geom_transfn);

Datum bytea_knn_geom_transfn(PG_FUNCTION_ARGS)
{
    lwdebug(5, "Enter bytea_knn_geom_transfn().");

    // Get the actual type OID of a specific function argument (counting from 0)
    Datum argType = get_fn_expr_argtype(fcinfo->flinfo, 1);
    if (argType == InvalidOid)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("could not determine input data type")));

    MemoryContext aggcontext;
    if (!AggCheckCallContext(fcinfo, &aggcontext)) {
        elog(ERROR, "bytea_knn_geom_transfn called in non-aggregate context");
        aggcontext = NULL;  /* keep compiler quiet */
    }

    KnnCollectionState* state;
    if ( PG_ARGISNULL(0) ) {
        // first incoming row/item
        state = (KnnCollectionState*)MemoryContextAlloc(aggcontext,
                sizeof(KnnCollectionState));
        state->geoms = NULL;
        state->ogc_fids = NULL;
        state->geomOid = argType;
        //MemoryContextSwitchTo(old);
    } else {
        state = (KnnCollectionState*) PG_GETARG_POINTER(0);
    }

    LWGEOM* geom;
    int idx;

    MemoryContext old = MemoryContextSwitchTo(aggcontext);

    if (!PG_ARGISNULL(1)) {
        idx = PG_GETARG_INT64(1);
    }

    if (!PG_ARGISNULL(2)) {
        bytea *bytea_wkb = PG_GETARG_BYTEA_P(2);
        uint8_t *wkb = (uint8_t *) VARDATA(bytea_wkb);
        LWGEOM *lwgeom = lwgeom_from_wkb(wkb, VARSIZE_ANY_EXHDR(bytea_wkb), LW_PARSER_CHECK_ALL);
        geom = lwgeom_clone_deep(lwgeom);
        lwgeom_free(lwgeom);
        //PG_FREE_IF_COPY(bytea_wkb, 0);
    }

    int k= 4;
    if (!PG_ARGISNULL(3)) k = PG_GETARG_INT64(3);
    state->k = k;

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

    PGWeight* w = create_knn_weights(p->ogc_fids, p->geoms, p->k);

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