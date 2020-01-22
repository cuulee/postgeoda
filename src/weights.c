#include <postgres.h>
#include <pg_config.h>
#include <fmgr.h>
#include <nodes/execnodes.h>
#include <funcapi.h>
#include <utils/array.h>
//#include <utils/geo_decls.h>
#include "utils/lsyscache.h" /* for get_typlenbyvalalign */

#include "config.h"
#include "geoms.h"
#include "proxy.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/* Argument handling macros */
#define PG_GETARG_GSERIALIZED_P(varno) ((GSERIALIZED *)PG_DETOAST_DATUM(PG_GETARG_DATUM(varno)))
#define PG_GETARG_GSERIALIZED_P_COPY(varno) ((GSERIALIZED *)PG_DETOAST_DATUM_COPY(PG_GETARG_DATUM(varno)))
#define PG_GETARG_GSERIALIZED_P_SLICE(varno, start, size) ((GSERIALIZED *)PG_DETOAST_DATUM_SLICE(PG_GETARG_DATUM(varno), start, size))

#define CollectionBuildStateDataSize 2
typedef struct CollectionBuildState
{
    List *geoms;  /* collected geometries */
    //Datum data[CollectionBuildStateDataSize];
    Oid geomOid;
} CollectionBuildState;

///////////////////////////////////////////////////////////////////////////////////////////
// test
PG_FUNCTION_INFO_V1(grt_sfunc);


typedef struct Point
{
    double x;
    double y;
} Point;

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
    Point *agg_state = PG_GETARG_POINTER(0);

    new_agg_state->x = agg_state->x + el;
    if(new_agg_state->x > agg_state->y) {
        new_agg_state->y = new_agg_state->x;
    } else {
        new_agg_state->y = agg_state->y;
    }

    PG_RETURN_POINTER(new_agg_state);
}


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

Datum pg_all_queries(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_all_queries);

Datum pg_all_queries(PG_FUNCTION_ARGS)
{
	return (Datum) 0;
}




///////////////////////////////////////////////////////////////////////////////////////////
// weights

/* Local prototypes */
Datum PGISDirectFunctionCall1(PGFunction func, Datum arg1);
/**
* A modified version of PostgreSQL's DirectFunctionCall1 which allows NULL results; this
* is required for aggregates that return NULL.
*/
Datum
PGISDirectFunctionCall1(PGFunction func, Datum arg1)
{
#if POSTGIS_PGSQL_VERSION < 120
    FunctionCallInfoData fcinfo;
    Datum           result;


    InitFunctionCallInfoData(fcinfo, NULL, 1, InvalidOid, NULL, NULL);


    fcinfo.arg[0] = arg1;
    fcinfo.argnull[0] = false;

    result = (*func) (&fcinfo);

    /* Check for null result, returning a "NULL" Datum if indicated */
    if (fcinfo.isnull)
        return (Datum) 0;

    return result;
#else
    LOCAL_FCINFO(fcinfo, 1);
	Datum result;

	InitFunctionCallInfoData(*fcinfo, NULL, 1, InvalidOid, NULL, NULL);

	fcinfo->args[0].value = arg1;
	fcinfo->args[0].isnull = false;

	result = (*func)(fcinfo);

	/* Check for null result, returning a "NULL" Datum if indicated */
	if (fcinfo->isnull)
		return (Datum)0;

	return result;
#endif /* POSTGIS_PGSQL_VERSION < 120 */
}

/* Local prototypes */
Datum pgis_accum_finalfn(CollectionBuildState *state, MemoryContext mctx, FunctionCallInfo fcinfo);

/**
** The final function reads the List of LWGEOM* from the aggregate
** memory context and constructs an Array using construct_md_array()
*/
Datum
pgis_accum_finalfn(CollectionBuildState *state, MemoryContext mctx, __attribute__((__unused__)) FunctionCallInfo fcinfo)
{
    ListCell *l;
    size_t nelems = 0;
    Datum *elems;
    bool *nulls;
    int16 elmlen;
    bool elmbyval;
    char elmalign;
    size_t i = 0;
    ArrayType *arr;
    int dims[1];
    int lbs[1] = {1};

    /* cannot be called directly because of internal-type argument */
    Assert(fcinfo->context &&
           (IsA(fcinfo->context, AggState) ||
            IsA(fcinfo->context, WindowAggState))
    );

    /* Retrieve geometry type metadata */
    get_typlenbyvalalign(state->geomOid, &elmlen, &elmbyval, &elmalign);
    nelems = list_length(state->geoms);

    /* Build up an array, because that's what we pass to all the */
    /* specific final functions */
    elems = palloc(nelems * sizeof(Datum));
    nulls = palloc(nelems * sizeof(bool));

    foreach (l, state->geoms)
    {
        LWGEOM *geom = (LWGEOM*)(lfirst(l));
        Datum elem = (Datum)0;
        bool isNull = true;
        if (geom)
        {
            GSERIALIZED *gser = geometry_serialize(geom);
            elem = PointerGetDatum(gser);
            isNull = false;
        }
        elems[i] = elem;
        nulls[i] = isNull;
        i++;

        if (i >= nelems)
            break;
    }

    /* Turn element array into PgSQL array */
    dims[0] = nelems;
    arr = construct_md_array(elems, nulls, 1, dims, lbs, state->geomOid,
                             elmlen, elmbyval, elmalign);

    return PointerGetDatum(arr);
}

/* External prototypes */

Datum bytea_to_geom_transfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(bytea_to_geom_transfn);

Datum bytea_to_geom_transfn(PG_FUNCTION_ARGS)
{
    // Get the actual type OID of a specific function argument (counting from 0)
    Datum argType = get_fn_expr_argtype(fcinfo->flinfo, 1);
    if (argType == InvalidOid)
		ereport(ERROR,
		        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		         errmsg("could not determine input data type")));
    
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
		state->geomOid = argType;
	} else {
		state = (CollectionBuildState*) PG_GETARG_POINTER(0);
	}

    GSERIALIZED *gser = NULL;
    if (!PG_ARGISNULL(1))
        gser = PG_GETARG_GSERIALIZED_P(1);

    LWGEOM *geom = NULL;
    /* Take a copy of the geometry into the aggregate context */
    MemoryContext old = MemoryContextSwitchTo(aggcontext);
    if (gser)
        geom = lwgeom_clone_deep(lwgeom_from_gserialized(gser));

    /* Initialize or append to list as necessary */
    if (state->geoms)
        state->geoms = lappend(state->geoms, geom); // pg_list
    else
        state->geoms = list_make1(geom);

    MemoryContextSwitchTo(old);

    PG_RETURN_POINTER(state);
}

/**
* The "collect" final function passes the geometry[] to a geometrycollection
* conversion before returning the result.
*/
Datum bytea_to_geom_finalfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(bytea_to_geom_finalfn);

Datum bytea_to_geom_finalfn(PG_FUNCTION_ARGS)
{
    CollectionBuildState *p;
    Datum result = 0;
    Datum geometry_array = 0;

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();   /* returns null iff no input values */

    p = (CollectionBuildState*) PG_GETARG_POINTER(0);

    ListCell *l;
    size_t nelems = 0;
    Datum *elems;
    bool *nulls;
    int16 elmlen;
    bool elmbyval;
    char elmalign;
    size_t i = 0;
    ArrayType *arr;
    int dims[1];
    int lbs[1] = {1};

    /* cannot be called directly because of internal-type argument */
    Assert(fcinfo->context &&
           (IsA(fcinfo->context, AggState) ||
            IsA(fcinfo->context, WindowAggState))
    );

    /* Retrieve geometry type metadata */
    get_typlenbyvalalign(p->geomOid, &elmlen, &elmbyval, &elmalign);
    nelems = list_length(p->geoms);

    /* Build up an array, because that's what we pass to all the */
    /* specific final functions */
    elems = palloc(nelems * sizeof(Datum));
    nulls = palloc(nelems * sizeof(bool));

    foreach (l, p->geoms)
    {
        LWGEOM *geom = (LWGEOM*)(lfirst(l));
        Datum elem = (Datum)0;
        bool isNull = true;
        if (geom)
        {
            GSERIALIZED *gser = geometry_serialize(geom);
            elem = PointerGetDatum(gser);
            isNull = false;
        }
        elems[i] = elem;
        nulls[i] = isNull;
        i++;

        if (i >= nelems)
            break;
    }

    /* Turn element array into PgSQL array */
    dims[0] = nelems;
    arr = construct_md_array(elems, nulls, 1, dims, lbs, p->geomOid,
                             elmlen, elmbyval, elmalign);

    geometry_array = pgis_accum_finalfn(p, CurrentMemoryContext, fcinfo);

    PGWeight* w = create_queen_weights(p->geomOid, p->geoms);

    PG_RETURN_NULL();

    //PG_RETURN_DATUM(result);
}


