#include <postgres.h>
#include <pg_config.h>
#include <fmgr.h>
#include <nodes/execnodes.h>
#include <funcapi.h>
#include <utils/array.h>
//#include <utils/geo_decls.h>

#include "config.h"
#include "geoms.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif


#define CollectionBuildStateDataSize 2
typedef struct CollectionBuildState
{
    List *geoms;  /* collected geometries */
    //Datum data[CollectionBuildStateDataSize];
    Oid geomOid;
} CollectionBuildState;

typedef struct Point
{
    double x;
    double y;
} Point;


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
    
    MemoryContext aggContext, old;
    if (!AggCheckCallContext(fcinfo, &aggContext)) {
        elog(ERROR, "bytea_to_geom_transfn called in non-aggregate context");
    }

    CollectionBuildState* state;
    if ( PG_ARGISNULL(0) )
	{
        // first incoming row/item
		state = (CollectionBuildState*)MemoryContextAlloc(aggContext, sizeof(CollectionBuildState));
		state->geoms = NULL;
		state->geomOid = argType;
	}
	else
	{
		state = (CollectionBuildState*) PG_GETARG_POINTER(0);
	}


    bytea *bytea_wkb = PG_GETARG_BYTEA_P(1);
    uint8_t *wkb = (uint8_t*)VARDATA(bytea_wkb);
    size_t wkb_size =  VARSIZE_ANY_EXHDR(bytea_wkb);
    char check = LW_PARSER_CHECK_ALL;

    LWGEOM *lwgeom = lwgeom_from_wkb(wkb, VARSIZE_ANY_EXHDR(bytea_wkb), LW_PARSER_CHECK_ALL);




    Point *new_agg_state = (Point *) palloc(sizeof(Point));
    double el = PG_GETARG_FLOAT8(1);
    bool isnull = PG_ARGISNULL(0);
    if(isnull) {
        new_agg_state->x = VARSIZE(bytea_wkb);
        new_agg_state->y = VARSIZE(bytea_wkb);
        PG_RETURN_POINTER(new_agg_state);
        //state = (ArrayBuildState *)MemoryContextAlloc(aggcontext, sizeof(ArrayBuildState));
        //state->mcontext = aggcontext;
    }

    //state = (ArrayBuildState*) PG_GETARG_POINTER(0);
    Point *agg_state = PG_GETARG_POINTER(0);

    new_agg_state->x = VARSIZE(bytea_wkb);
    new_agg_state->y = VARSIZE(bytea_wkb);


    PG_RETURN_POINTER(agg_state);
}


Datum bytea_to_geom_finalfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(bytea_to_geom_finalfn);

Datum bytea_to_geom_finalfn(PG_FUNCTION_ARGS)
{
    Point *agg_state = PG_GETARG_POINTER(0);

    PG_RETURN_INT32(1);
}

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
  Point *agg_state = PG_GETARG_POINTER(0);

  new_agg_state->x = agg_state->x + el;
  if(new_agg_state->x > agg_state->y) {
    new_agg_state->y = new_agg_state->x;
  } else {
    new_agg_state->y = agg_state->y;
  }

  PG_RETURN_POINTER(new_agg_state);
}