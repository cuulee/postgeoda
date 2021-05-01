/**
 * Author: Xun Li <lixun910@gmail.com>
 *
 * Changes:
 * 2021-4-29 add CollectVariableState; add variable_transfn(); add hinge15_finalfn()
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

typedef struct CollectVariableState
{
    List *data;  /* collected values */
    List *undefs;
    int k; //optional
} CollectVariableState;

/**
 * variable_transfn()
 *
 * This function collects the values of input variable for AGGREGATE SQL function.
 *
 * @param fcinfo
 * @return
 */
Datum variable_transfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(variable_transfn);
Datum variable_transfn(PG_FUNCTION_ARGS) {
    //lwdebug(1,"Enter variable_transfn.");

    // Get the actual type OID of a specific function argument (counting from 0)
    Datum argType = get_fn_expr_argtype(fcinfo->flinfo, 1);
    if (argType == InvalidOid) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("could not determine input data type")));
    }

    MemoryContext aggcontext;
    if (!AggCheckCallContext(fcinfo, &aggcontext)) {
        elog(ERROR, "variable_transfn called in non-aggregate context");
        aggcontext = NULL;  /* keep compiler quiet */
    }

    CollectVariableState* state;
    if ( PG_ARGISNULL(0) ) {
        // first incoming row/item
        state = (CollectVariableState*)MemoryContextAlloc(aggcontext, sizeof(CollectVariableState));
        state->data = NULL;
        state->undefs = NULL;
        state->k = 4;
        //MemoryContextSwitchTo(old);
    } else {
        state = (CollectVariableState*) PG_GETARG_POINTER(0);
    }

    double *val = (double*)lwalloc(sizeof(double));
    bool *undef= (bool*)lwalloc(sizeof(bool));

    /* Take a copy of the geometry into the aggregate context */
    MemoryContext old = MemoryContextSwitchTo(aggcontext);

    // values
    if (!PG_ARGISNULL(1)) {
        val[0] = get_numeric_val(argType, PG_GETARG_DATUM(1));
        undef[0] = false;
        //lwdebug(1,"variable_transfn: value: %f", val[0]);
    } else {
        val[0] = 0;
        undef[0] = true;
        lwdebug(1,"variable_transfn: NULL value");
    }

    // k (optional)
    if (!PG_ARGISNULL(2)) {
        state->k = PG_GETARG_INT64(2);
    }

    /* Initialize or append to list as necessary */
    if (state->data) {
        lwdebug(4, "lappend val.");
        state->data = lappend(state->data, val);
        state->undefs = lappend(state->undefs, undef);
    } else {
        lwdebug(4, "list_make.");
        state->data = list_make1(val);
        state->undefs = list_make1(undef);
    }

    MemoryContextSwitchTo(old);

    PG_RETURN_POINTER(state);
}

Datum hinge15_finalfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(hinge15_finalfn);
Datum hinge15_finalfn(PG_FUNCTION_ARGS)
{
    CollectVariableState *p;

    lwdebug(1,"Enter hinge15_finalfn.");

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();   /* returns null iff no input values */
    }

    // get State from aggregate internal function
    p = (CollectVariableState*) PG_GETARG_POINTER(0);

    int nelems = 5;
    double *breaks = pg_hinge_aggregate(p->data, p->undefs, true);

    /* Prepare the PgSQL text return type */
    Datum elems[nelems];
    for (int i=0; i<nelems; ++i) {
        elems[i] = Float8GetDatum(breaks[i]);
    }

    /* Clean up and return */
    pfree(breaks);

    Oid elmtype = FLOAT8OID;
    int16 elmlen;
    bool elmbyval;
    char elmalign;
    get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);
    ArrayType *array = construct_array(elems, nelems, elmtype, elmlen, elmbyval, elmalign);

    lwdebug(1,"Exit hinge15_finalfn.");
    PG_RETURN_ARRAYTYPE_P(array);
}


Datum hinge30_finalfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(hinge30_finalfn);
Datum hinge30_finalfn(PG_FUNCTION_ARGS)
{
    CollectVariableState *p;

    lwdebug(1,"Enter hinge30_finalfn.");

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();   /* returns null iff no input values */
    }

    // get State from aggregate internal function
    p = (CollectVariableState*) PG_GETARG_POINTER(0);

    List *data = p->data;
    int nelems = 5;
    double *breaks = pg_hinge_aggregate(data, p->undefs, false);

    /* Prepare the PgSQL text return type */
    Datum elems[nelems];
    for (int i=0; i<nelems; ++i) {
        elems[i] = Float8GetDatum(breaks[i]);
    }

    /* Clean up and return */
    lwfree(breaks);

    Oid elmtype = FLOAT8OID;
    int16 elmlen;
    bool elmbyval;
    char elmalign;
    get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);
    ArrayType *array = construct_array(elems, nelems, elmtype, elmlen, elmbyval, elmalign);

    lwdebug(1,"Exit hinge30_finalfn.");
    PG_RETURN_ARRAYTYPE_P(array);
}

Datum percentile_finalfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(percentile_finalfn);
Datum percentile_finalfn(PG_FUNCTION_ARGS)
{
    CollectVariableState *p;

    lwdebug(1,"Enter percentile_finalfn.");

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();   /* returns null iff no input values */
    }

    // get State from aggregate internal function
    p = (CollectVariableState*) PG_GETARG_POINTER(0);

    List *data = p->data;
    int nelems = 5;
    double *breaks = pg_percentile_aggregate(data, p->undefs, &nelems);

    /* Prepare the PgSQL text return type */
    Datum elems[nelems];
    for (int i=0; i<nelems; ++i) {
        elems[i] = Float8GetDatum(breaks[i]);
    }

    /* Clean up and return */
    lwfree(breaks);

    Oid elmtype = FLOAT8OID;
    int16 elmlen;
    bool elmbyval;
    char elmalign;
    get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);
    ArrayType *array = construct_array(elems, nelems, elmtype, elmlen, elmbyval, elmalign);

    lwdebug(1,"Exit percentile_finalfn.");
    PG_RETURN_ARRAYTYPE_P(array);
}


Datum stddev_finalfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(stddev_finalfn);
Datum stddev_finalfn(PG_FUNCTION_ARGS)
{
    CollectVariableState *p;

    lwdebug(1,"Enter stddev_finalfn.");

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();   /* returns null iff no input values */
    }

    // get State from aggregate internal function
    p = (CollectVariableState*) PG_GETARG_POINTER(0);

    List *data = p->data;
    int nelems = 0;
    double *breaks = pg_stddev_aggregate(data, p->undefs, &nelems);

    /* Prepare the PgSQL text return type */
    Datum elems[nelems];
    for (int i=0; i<nelems; ++i) {
        elems[i] = Float8GetDatum(breaks[i]);
    }

    /* Clean up and return */
    lwfree(breaks);

    Oid elmtype = FLOAT8OID;
    int16 elmlen;
    bool elmbyval;
    char elmalign;
    get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);
    ArrayType *array = construct_array(elems, nelems, elmtype, elmlen, elmbyval, elmalign);

    lwdebug(1,"Exit stddev_finalfn.");
    PG_RETURN_ARRAYTYPE_P(array);
}


Datum quantile_finalfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(quantile_finalfn);
Datum quantile_finalfn(PG_FUNCTION_ARGS)
{
    CollectVariableState *p;

    lwdebug(1,"Enter quantile_finalfn.");

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();   /* returns null iff no input values */
    }

    // get State from aggregate internal function
    p = (CollectVariableState*) PG_GETARG_POINTER(0);

    List *data = p->data;
    int nelems = p->k - 1;
    double *breaks = pg_quantile_aggregate(data, p->undefs, p->k);

    /* Prepare the PgSQL text return type */
    Datum elems[nelems];
    for (int i=0; i<nelems; ++i) {
        elems[i] = Float8GetDatum(breaks[i]);
    }

    /* Clean up and return */
    lwfree(breaks);

    Oid elmtype = FLOAT8OID;
    int16 elmlen;
    bool elmbyval;
    char elmalign;
    get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);
    ArrayType *array = construct_array(elems, nelems, elmtype, elmlen, elmbyval, elmalign);

    lwdebug(1,"Exit quantile_finalfn.");
    PG_RETURN_ARRAYTYPE_P(array);
}

Datum naturalbreaks_finalfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(naturalbreaks_finalfn);
Datum naturalbreaks_finalfn(PG_FUNCTION_ARGS)
{
    CollectVariableState *p;

    lwdebug(1,"Enter naturalbreaks_finalfn.");

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();   /* returns null iff no input values */
    }

    // get State from aggregate internal function
    p = (CollectVariableState*) PG_GETARG_POINTER(0);

    List *data = p->data;
    int nelems = p->k - 1;
    double *breaks = pg_naturalbreaks_aggregate(data, p->undefs, p->k);

    /* Prepare the PgSQL text return type */
    Datum elems[nelems];
    for (int i=0; i<nelems; ++i) {
        elems[i] = Float8GetDatum(breaks[i]);
    }

    /* Clean up and return */
    lwfree(breaks);

    Oid elmtype = FLOAT8OID;
    int16 elmlen;
    bool elmbyval;
    char elmalign;
    get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);
    ArrayType *array = construct_array(elems, nelems, elmtype, elmlen, elmbyval, elmalign);

    lwdebug(1,"Exit naturalbreaks_finalfn.");
    PG_RETURN_ARRAYTYPE_P(array);
}
#ifdef __cplusplus
}
#endif

