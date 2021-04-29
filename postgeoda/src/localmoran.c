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

#include <libgeoda/pg/utils.h>
#include <libgeoda/pg/geoms.h>
#include "proxy.h"
#include "lisa.h"

#ifndef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/**
 * pg_local_moran_window()
 *
 * The Window function for local_moran()
 * @param fcinfo
 * @return
 */
Datum pg_local_moran_window(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pg_local_moran_window);
Datum pg_local_moran_window(PG_FUNCTION_ARGS) {
    WindowObject winobj = PG_WINDOW_OBJECT();
    lisa_context *context;
    int64 curpos, rowcount;

    Oid valsType = get_fn_expr_argtype(fcinfo->flinfo, 0);
    check_if_numeric_type(valsType);

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

        // read data
        uint8_t **w = lwalloc(sizeof(uint8_t *) * N);
        size_t *w_size = lwalloc(sizeof(size_t) * N);
        double *r = lwalloc(sizeof(double) * N);

        lwdebug(0, "Init local_moran_window. N=%d", N);

        for (size_t i = 0; i < N; i++) {
            Datum arg = WinGetFuncArgInPartition(winobj, 0, i,
                                                 WINDOW_SEEK_HEAD, false, &isnull, &isout);
            r[i] = get_numeric_val(valsType, arg);
            Datum arg1 = WinGetFuncArgInPartition(winobj, 1, i,
                                                  WINDOW_SEEK_HEAD, false, &isnull, &isout);
            bytea *w_bytea = DatumGetByteaP(arg1); //shallow copy
            //bytea *w_bytea = (bytea*)PG_DETOAST_DATUM_COPY(arg1);
            uint8_t *w_val = (uint8_t *) VARDATA(w_bytea);
            w[i] = w_val;
            w_size[i] = VARSIZE_ANY_EXHDR(w_bytea);
        }

        // read arguments
        int arg_index = 2;
        lisa_arguments args = {999, 0, 0.05, 6, 123456789};

        read_lisa_arguments(arg_index, PG_NARGS(), winobj, &args);

        double **result = local_moran_window(N, r, (const uint8_t**)w, w_size, args.permutations, args.method,
                                            args.significance_cutoff, args.cpu_threads, args.seed);

        // Safe the result
        context->result = result;
        context->isdone = true;

        // clean
        lwdebug(1, "Clean local_moran_window.");
        lwfree(r);
        lwfree(w_size);
        lwfree(w);

        lwdebug(1, "Exit local_moran_window.");
    }

    if (context->isnull)
        PG_RETURN_NULL();

    curpos = WinGetCurrentPosition(winobj);

    // Wrap the results in a new PostgreSQL array object.
    double *p = context->result[curpos];
    Datum elems[3];
    elems[0] = Float8GetDatum(p[0]); // double to Datum
    elems[1] = Float8GetDatum(p[1]);
    elems[2] = Float8GetDatum(p[2]);
    int nelems = 3;
    Oid elmtype = FLOAT8OID;
    int16 elmlen;
    bool elmbyval;
    char elmalign;
    get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);
    ArrayType *array = construct_array(elems, nelems, elmtype, elmlen, elmbyval, elmalign);

    PG_RETURN_ARRAYTYPE_P(array);
}

/**
 * pg_local_moran_fast()
 *
 * This Window function will take an extra input (compare to local_moran()), which is the
 * values of the selected variable in the form of ANYARRAY, to do local moran computation
 * use the data in current window.
 *
 * The whole data can be divided into several windows, so local_moran_fast() can be distributed
 * on more than one PG nodes to process the window function distributedly and in parallel.
 *
 * @param fcinfo
 * @return
 */
Datum pg_local_moran_fast(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_local_moran_fast);

Datum pg_local_moran_fast(PG_FUNCTION_ARGS)
{
    lwdebug(1, "Enter pg_local_moran_fast.");

    if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2) ) {
        ereport(ERROR, (errmsg("Null arguments not accepted")));
    }

    // 1st arg
    Oid  valType = get_fn_expr_argtype(fcinfo->flinfo, 0);
    check_if_numeric_type(valType);

    Datum arg = PG_GETARG_DATUM(0);
    double val = get_numeric_val(valType, arg);

    lwdebug(1, "pg_local_moran_fast. first arg=%f", val);

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

    lwdebug(1, "pg_local_moran_fast. arr_size = %d", arr_size);

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
    Point *r = local_moran_fast(val, w_val, bw_size, arr_size, arr, permutations, rnd_seed);

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

/**
 *  local_moran_window_bytea (This is going to be depreciated)
 *
 *  Unlike local_moran_window, this Window function requires the input weights to be
 *  a whole part (e.g. using queen_weights_b) in BYTEA format.
 *
 * @param fcinfo
 * @return
 */
Datum pg_local_moran_window_bytea(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_local_moran_window_bytea);
Datum pg_local_moran_window_bytea(PG_FUNCTION_ARGS) {
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
            Datum arg = WinGetFuncArgInPartition(winobj, 0, i, WINDOW_SEEK_HEAD, false, &isnull, &isout);
            fids[i] = DatumGetInt64(arg);
            //lwdebug(1, "local_moran_window_bytea: %d-th:%d", i, fids[i]);
        }

        // Read all the values from the partition window into a list
        Oid  valsType = get_fn_expr_argtype(fcinfo->flinfo, 1);
        check_if_numeric_type(valsType);

        double *r = lwalloc(sizeof(double) * N);
        for (size_t i = 0; i < N; i++) {
            Datum arg = WinGetFuncArgInPartition(winobj, 1, i, WINDOW_SEEK_HEAD, false, &isnull, &isout);
            r[i] = get_numeric_val(valsType, arg);
            //lwdebug(1, "local_moran_window_bytea: %d-th:%f", i, r[i]);
        }

        // compute lisa
        double **result = local_moran_window_bytea(N, fids, r, w);

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

// Try to utilize the indexing of postgresql
// Possible speed up when partitioning tables into different parts
// that can be running on several nodes, and then being combined
// into one result
Datum postgis_index_supportfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(postgis_index_supportfn);
Datum postgis_index_supportfn(PG_FUNCTION_ARGS)
{
    //Node *rawreq = (Node *) PG_GETARG_POINTER(0);
    Node *ret = NULL;
    PG_RETURN_POINTER(ret);
}

#ifdef __cplusplus
}
#endif
