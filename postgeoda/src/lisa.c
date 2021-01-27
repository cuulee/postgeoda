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

#ifndef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

void check_if_numeric_type(Oid  valsType)
{
    if (valsType != INT2OID &&
        valsType != INT4OID &&
        valsType != INT8OID &&
        valsType != FLOAT4OID &&
        valsType != FLOAT8OID) {
        ereport(ERROR, (errmsg("localmoran first parameter must be SMALLINT, INTEGER, BIGINT, REAL, or DOUBLE "
                               "PRECISION "
                               "values")));
    }
}

double get_numeric_val(Oid valsType, Datum arg)
{
    double val = 0;
    switch (valsType) {
        case INT2OID:
            val = DatumGetInt16(arg);
            break;
        case INT4OID:
            val = DatumGetInt32(arg);
            break;
        case INT8OID:
            val = DatumGetInt64(arg);
            break;
        case FLOAT4OID:
            val = DatumGetFloat4(arg);
            break;
        case FLOAT8OID:
            val = DatumGetFloat8(arg);
            break;
        default:
            break;
    }
    return val;
}

Datum local_moran_window_bytea(PG_FUNCTION_ARGS);

typedef struct {
    bool	isdone;
    bool	isnull;
    Point  **result;
    /* variable length */
} lisa_context;

PG_FUNCTION_INFO_V1(local_moran_window_bytea);
Datum local_moran_window_bytea(PG_FUNCTION_ARGS) {
    WindowObject winobj = PG_WINDOW_OBJECT();
    lisa_context *context;
    int64 curpos, rowcount;

    rowcount = WinGetPartitionRowCount(winobj);
    context = (lisa_context *)WinGetPartitionLocalMemory(winobj,
                                                         sizeof(lisa_context) + sizeof(int) * rowcount);

    if (!context->isdone) {
        bool isnull, isout;

        /* We also need a non-zero N */
        int N = (int) WinGetPartitionRowCount(winobj);
        if (N <= 0) {
            context->isdone = true;
            context->isnull = true;
            PG_RETURN_NULL();
        }

        // read  weights in bytea
        //bytea *bw = DatumGetByteaP(WinGetFuncArgCurrent(winobj, 1, &isnull));
        Datum arg_b = WinGetFuncArgCurrent(winobj, 2, &isnull);
        bytea *bw = (bytea*)PG_DETOAST_DATUM_COPY(arg_b);
        if (isnull) {
            PG_RETURN_NULL();
        }
        uint8_t *w = (uint8_t*)VARDATA(bw);

        // read fids
        int64 *fids = lwalloc(sizeof(int64) * N);
        for (size_t i = 0; i < N; i++) {
            Datum arg = WinGetFuncArgInPartition(winobj, 0, i,
                                                 WINDOW_SEEK_HEAD, false, &isnull, &isout);
            fids[i] = DatumGetInt64(arg);
            //lwdebug(1, "local_moran_window_bytea: %d-th:%d", i, fids[i]);
        }

        // Read all the values from the partition window into a list
        Oid  valsType = get_fn_expr_argtype(fcinfo->flinfo, 1);
        check_if_numeric_type(valsType);

        double val, *r = lwalloc(sizeof(double) * N);
        for (size_t i = 0; i < N; i++) {
            Datum arg = WinGetFuncArgInPartition(winobj, 1, i,
                                                 WINDOW_SEEK_HEAD, false, &isnull, &isout);
            r[i] = get_numeric_val(valsType, arg);
            //lwdebug(1, "local_moran_window_bytea: %d-th:%f", i, r[i]);
        }

        // compute lisa
        Point **result = pg_local_moran(N, fids, r, w);

        // Safe the result
        context->result = result;
        context->isdone = true;

        // Clean
        lwfree(r);
        PG_FREE_IF_COPY(bw, 0);

        lwdebug(1, "Exit local_moran_window_bytea. free_lisa() done.");
    }

    if (context->isnull)
        PG_RETURN_NULL();

    curpos = WinGetCurrentPosition(winobj);
    PG_RETURN_POINT_P(context->result[curpos]);
}

Datum local_moran_window(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(local_moran_window);
Datum local_moran_window(PG_FUNCTION_ARGS) {
    WindowObject winobj = PG_WINDOW_OBJECT();
    lisa_context *context;
    int64 curpos, rowcount;

    Oid  valsType = get_fn_expr_argtype(fcinfo->flinfo, 0);
    check_if_numeric_type(valsType);

    rowcount = WinGetPartitionRowCount(winobj);
    context = (lisa_context *)
            WinGetPartitionLocalMemory(winobj,
                                       sizeof(lisa_context) + sizeof(int) * rowcount);

    if (!context->isdone) {
        bool isnull, isout;

        /* We also need a non-zero N */
        int N = (int) WinGetPartitionRowCount(winobj);
        if (N <= 0) {
            context->isdone = true;
            context->isnull = true;
            PG_RETURN_NULL();
        }

        bytea **wb_copy = lwalloc(sizeof(bytea*) * N);
        uint8_t **w = lwalloc(sizeof(uint8_t *) * N);
        size_t *w_size = lwalloc(sizeof(size_t) * N);
        double val, *r = lwalloc(sizeof(double) * N);

        for (size_t i = 0; i < N; i++) {
            Datum arg = WinGetFuncArgInPartition(winobj, 0, i,
                                                 WINDOW_SEEK_HEAD, false, &isnull, &isout);
            r[i] = get_numeric_val(valsType, arg);
            Datum arg1 = WinGetFuncArgInPartition(winobj, 1, i,
                                                  WINDOW_SEEK_HEAD, false, &isnull, &isout);
            //bytea *w_bytea = DatumGetByteaP(arg1);
            bytea *w_bytea = (bytea*)PG_DETOAST_DATUM_COPY(arg1);
            uint8_t *w_val = (uint8_t *) VARDATA(w_bytea);
            w[i] = w_val;
            w_size[i] = VARSIZE_ANY_EXHDR(w_val);
            wb_copy[i] = w_bytea;
        }

        lwdebug(1, "Enter local_moran_window. N=%d", N);
        Point **result = pg_local_moran_warray(N, r, w, w_size);

        // Safe the result
        context->result = result;
        context->isdone = true;

        // clean
        lwfree(r);
        for (size_t i=0; i<N; ++i) PG_FREE_IF_COPY(wb_copy[i], 0);
        lwfree(wb_copy);
        lwfree(w_size);

        lwdebug(1, "Exit local_moran_window. free_lisa() done.");
    }

    if (context->isnull)
        PG_RETURN_NULL();

    curpos = WinGetCurrentPosition(winobj);
    PG_RETURN_POINT_P(context->result[curpos]);
}

Datum local_moran_fast(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(local_moran_fast);

Datum local_moran_fast(PG_FUNCTION_ARGS)
{
    lwdebug(1, "Enter geoda_localmoran_fast.");

    if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2) ) {
        ereport(ERROR, (errmsg("Null arguments not accepted")));
    }

    // 1st arg
    Oid  valType = get_fn_expr_argtype(fcinfo->flinfo, 0);
    check_if_numeric_type(valType);

    Datum arg = PG_GETARG_DATUM(0);
    double val = get_numeric_val(valType, arg);

    lwdebug(1, "geoda_localmoran_fast. first arg=%f", val);

    // 2nd arg
    bytea *w_bytea = (bytea*)PG_DETOAST_DATUM_COPY(PG_GETARG_DATUM(1));
    uint8_t *w_val = (uint8_t *) VARDATA(w_bytea);
    size_t bw_size = VARSIZE_ANY_EXHDR(w_bytea);

    // 3rd arg
    ArrayType *vals;
    vals = PG_GETARG_ARRAYTYPE_P(2);

    if (ARR_NDIM(vals) > 1) {
        ereport(ERROR, (errmsg("One-dimesional arrays are required")));
    }

    Oid  valsType;
    valsType = ARR_ELEMTYPE(vals);
    check_if_numeric_type(valType);
    int arr_size = (ARR_DIMS(vals))[0];

    lwdebug(1, "geoda_localmoran_fast. arr_size = %d", arr_size);

    // get values from 3rd arg
    int16 valsTypeWidth;
    bool valsTypeByValue;
    char valsTypeAlignmentCode;
    bool *valsNullFlags;
    Datum *valsContent;

    get_typlenbyvalalign(valsType, &valsTypeWidth, &valsTypeByValue, &valsTypeAlignmentCode);

    // Extract the array contents (as Datum objects).
    deconstruct_array(vals, valsType, valsTypeWidth, valsTypeByValue, valsTypeAlignmentCode,
                      &valsContent, &valsNullFlags, &arr_size);

    double *arr = lwalloc(sizeof(double) * arr_size);

    for (size_t i=0; i<arr_size; ++i) {
        arr[i] = get_numeric_val(valsType, valsContent[i]);
    }

    // compute lisa
    int permutations = 999;
    int rnd_seed = 123456789;
    Point *r = pg_local_moran_fast(val, w_val, bw_size, arr_size, arr, permutations, rnd_seed);

    // clean
    PG_FREE_IF_COPY(w_bytea, 0);
    lwfree(arr);

    // return
    if (r == NULL) {
        ereport(ERROR, (errmsg("pg_local_moran_fast() error.")));
        PG_RETURN_NULL();
    }

    PG_RETURN_POINT_P(r);
}

Datum postgis_index_supportfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(postgis_index_supportfn);
Datum postgis_index_supportfn(PG_FUNCTION_ARGS)
{
    Node *rawreq = (Node *) PG_GETARG_POINTER(0);
    Node *ret = NULL;
    PG_RETURN_POINTER(ret);
}

#ifdef __cplusplus
}
#endif
