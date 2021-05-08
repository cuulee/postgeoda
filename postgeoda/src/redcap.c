/**
 * Author: Xun Li <lixun910@gmail.com>
 *
 * Changes:
 * 2021-5-1 add pg_redcap1_window(), pg_redcap2_window(), pg_redcap3_window()
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


Datum pg_redcap1_window(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_redcap1_window);
Datum pg_redcap1_window(PG_FUNCTION_ARGS) {
    WindowObject winobj = PG_WINDOW_OBJECT();
    scc_context *context;
    int64 curpos, rowcount;

    rowcount = WinGetPartitionRowCount(winobj);
    context = (scc_context *)WinGetPartitionLocalMemory(winobj, sizeof(scc_context) + sizeof(int) * rowcount);

    if (!context->isdone) {
        bool isnull, isout;

        /* We also need a non-zero N */
        int N = (int) WinGetPartitionRowCount(winobj);
        if (N <= 0) {
            context->isdone = true;
            context->isnull = true;
            PG_RETURN_NULL();
        }

        // read data and weights
        uint8_t **w = lwalloc(sizeof(uint8_t *) * N);
        size_t *w_size = lwalloc(sizeof(size_t) * N);
        double **r =  lwalloc(sizeof(double) * N);

        // get meta-data of input Array
        ArrayType *array;
        Oid arrayElementType;
        int16 arrayElementTypeWidth; //The array element type widths (should always be 4)
        // // The array element type "is passed by value" flags (not used, should always be true):
        bool arrayElementTypeByValue;
        // The array element type alignment codes (not used):
        char arrayElementTypeAlignmentCode;
        // The array contents, as PostgreSQL "datum" objects:
        Datum *arrayContent;
        // List of "is null" flags for the array contents:
        bool *arrayNullFlags;
        // The size of each array:
        int arrayLength;

        int arg_idx_data = 1, arg_idx_weights = 2;

        for (size_t i = 0; i < N; i++) {
            Datum arg0 = WinGetFuncArgInPartition(winobj, arg_idx_data, i, WINDOW_SEEK_HEAD, false, &isnull, &isout);
            if (isnull) {
                PG_RETURN_NULL();
            }
            array = DatumGetArrayTypeP(arg0);
            if (i == 0) {
                array = DatumGetArrayTypeP(arg0);
                arrayElementType = ARR_ELEMTYPE(array);
                check_if_numeric_type(arrayElementType);
                get_typlenbyvalalign(arrayElementType, &arrayElementTypeWidth, &arrayElementTypeByValue,
                                     &arrayElementTypeAlignmentCode);
            }
            // Extract the array contents (as Datum objects).
            deconstruct_array(array, arrayElementType, arrayElementTypeWidth, arrayElementTypeByValue,
                              arrayElementTypeAlignmentCode, &arrayContent, &arrayNullFlags, &arrayLength);
            r[i] = lwalloc(sizeof(double) * arrayLength);
            for (size_t j = 0; j < arrayLength; ++j) {
                r[i][j] = get_numeric_val(arrayElementType, arrayContent[j]);
            }

            Datum arg1 = WinGetFuncArgInPartition(winobj, arg_idx_weights, i, WINDOW_SEEK_HEAD, false, &isnull, &isout);
            if (isnull) {
                PG_RETURN_NULL();
            }
            bytea *w_bytea = DatumGetByteaP(arg1); //shallow copy
            uint8_t *w_val = (uint8_t *) VARDATA(w_bytea);
            w[i] = w_val;
            w_size[i] = VARSIZE_ANY_EXHDR(w_bytea);
        }

        // read arguments
        // k
        int k = DatumGetInt16(WinGetFuncArgCurrent(winobj, 0, &isnull));
        if (isnull || k < 0) {
            elog(ERROR, "redcap: k should be a positive integer number.");
        }

        char* redcap_method = 0;
        VarChar *arg = (VarChar *)DatumGetVarCharPP(WinGetFuncArgCurrent(winobj, 3, &isnull));
        redcap_method = (char *)VARDATA(arg);
        if (!check_redcap_method(redcap_method)) {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("redcap method should be one of: \"firstorder-singlelinkage\", \"fullorder-completelinkage\", \"fullorder-averagelinkage\",\"fullorder-singlelinkage\", \"fullorder-wardlinkage\"")));
        }

        lwdebug(1, "pg_redcap1_window. k=%d method=%s", k, redcap_method);

        int arg_idx = 4; // other optional args

        // min_region_size
        int min_region = 0;
        if (arg_idx < PG_NARGS()) {
            min_region = DatumGetInt16(WinGetFuncArgCurrent(winobj, arg_idx, &isnull));
        }
        arg_idx += 1;

        // scale_method
        char *scale_method = 0;
        if (arg_idx < PG_NARGS()) {
            scale_method = get_scale_method_arg(winobj, arg_idx);
        }
        arg_idx += 1;

        // dist type
        char *dist_type = 0;
        if (arg_idx < PG_NARGS()) {
            dist_type = get_distance_type_arg(winobj, arg_idx);
        }
        arg_idx += 1;

        // seed
        int seed = 123456789;
        if (arg_idx < PG_NARGS()) {
            seed = DatumGetInt32(WinGetFuncArgCurrent(winobj, arg_idx, &isnull));
        }
        arg_idx += 1;

        // cpu threads
        int cpu_threads = 6;
        if (arg_idx < PG_NARGS()) {
            cpu_threads = DatumGetInt32(WinGetFuncArgCurrent(winobj, arg_idx, &isnull));
        }
        arg_idx += 1;

        // call redcap
        int *result = redcap1_window(k, N, arrayLength, (const double**)r, (const uint8_t**)w, w_size, min_region,
                                     redcap_method, (const char*)scale_method, (const char*)dist_type, seed, cpu_threads);

        // Clean
        for (int i=0; i<N; ++i) lwfree(r[i]);
        lwfree(r);
        lwfree(w_size);
        lwfree(w);

        if (result == 0) {
            elog(ERROR, "redcap: can't find clusters. Please check if the connectivity of input spatial weights "
                        "is complete.");
        }
        // Safe the result
        context->result = result;
        context->isdone = true;

        lwdebug(1, "Exit pg_redcap_window. free_lisa() done.");
    }

    if (context->isnull)
        PG_RETURN_NULL();

    curpos = WinGetCurrentPosition(winobj);
    PG_RETURN_INT16(context->result[curpos]);
}

Datum pg_redcap2_window(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_redcap2_window);
Datum pg_redcap2_window(PG_FUNCTION_ARGS) {
    WindowObject winobj = PG_WINDOW_OBJECT();
    scc_context *context;
    int64 curpos, rowcount;

    rowcount = WinGetPartitionRowCount(winobj);
    context = (scc_context *)WinGetPartitionLocalMemory(winobj, sizeof(scc_context) + sizeof(int) * rowcount);

    if (!context->isdone) {
        bool isnull, isout;

        /* We also need a non-zero N */
        int N = (int) WinGetPartitionRowCount(winobj);
        if (N <= 0) {
            context->isdone = true;
            context->isnull = true;
            PG_RETURN_NULL();
        }

        // read data and weights
        uint8_t **w = lwalloc(sizeof(uint8_t *) * N);
        size_t *w_size = lwalloc(sizeof(size_t) * N);
        double **r =  lwalloc(sizeof(double) * N);

        // get meta-data of input Array
        ArrayType *array;
        Oid arrayElementType;
        int16 arrayElementTypeWidth; //The array element type widths (should always be 4)
        // // The array element type "is passed by value" flags (not used, should always be true):
        bool arrayElementTypeByValue;
        // The array element type alignment codes (not used):
        char arrayElementTypeAlignmentCode;
        // The array contents, as PostgreSQL "datum" objects:
        Datum *arrayContent;
        // List of "is null" flags for the array contents:
        bool *arrayNullFlags;
        // The size of each array:
        int arrayLength;

        int arg_idx_data = 1, arg_idx_weights = 2;

        for (size_t i = 0; i < N; i++) {
            Datum arg0 = WinGetFuncArgInPartition(winobj, arg_idx_data, i, WINDOW_SEEK_HEAD, false, &isnull, &isout);
            if (isnull) {
                PG_RETURN_NULL();
            }
            array = DatumGetArrayTypeP(arg0);
            if (i == 0) {
                array = DatumGetArrayTypeP(arg0);
                arrayElementType = ARR_ELEMTYPE(array);
                check_if_numeric_type(arrayElementType);
                get_typlenbyvalalign(arrayElementType, &arrayElementTypeWidth, &arrayElementTypeByValue,
                                     &arrayElementTypeAlignmentCode);
            }
            // Extract the array contents (as Datum objects).
            deconstruct_array(array, arrayElementType, arrayElementTypeWidth, arrayElementTypeByValue,
                              arrayElementTypeAlignmentCode, &arrayContent, &arrayNullFlags, &arrayLength);
            r[i] = lwalloc(sizeof(double) * arrayLength);
            for (size_t j = 0; j < arrayLength; ++j) {
                r[i][j] = get_numeric_val(arrayElementType, arrayContent[j]);
            }

            Datum arg1 = WinGetFuncArgInPartition(winobj, arg_idx_weights, i, WINDOW_SEEK_HEAD, false, &isnull, &isout);
            if (isnull) {
                PG_RETURN_NULL();
            }
            bytea *w_bytea = DatumGetByteaP(arg1); //shallow copy
            uint8_t *w_val = (uint8_t *) VARDATA(w_bytea);
            w[i] = w_val;
            w_size[i] = VARSIZE_ANY_EXHDR(w_bytea);
        }

        // read arguments
        // k
        int k = DatumGetInt16(WinGetFuncArgCurrent(winobj, 0, &isnull));
        if (isnull || k < 0) {
            elog(ERROR, "redcap: k should be a positive integer number.");
        }

        char* redcap_method = 0;
        VarChar *arg = (VarChar *)DatumGetVarCharPP(WinGetFuncArgCurrent(winobj, 3, &isnull));
        redcap_method = (char *)VARDATA(arg);
        if (!check_redcap_method(redcap_method)) {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("redcap method should be one of: \"firstorder-singlelinkage\", \"fullorder-completelinkage\", \"fullorder-averagelinkage\",\"fullorder-singlelinkage\", \"fullorder-wardlinkage\"")));
        }

        lwdebug(1, "pg_redcap2_window. k=%d method=%s", k, redcap_method);

        int arg_idx = 4; // other optional args

        // min bound variable
        double *bound_var  = 0;
        if (arg_idx < PG_NARGS()) {
            Oid valsType = get_fn_expr_argtype(fcinfo->flinfo, arg_idx);
            check_if_numeric_type(valsType);
            bound_var = (double*) lwalloc(sizeof(double) * N);
            for (size_t i = 0; i < N; i++) {
                Datum arg = WinGetFuncArgInPartition(winobj, arg_idx, i, WINDOW_SEEK_HEAD, false, &isnull, &isout);
                bound_var[i] = get_numeric_val(valsType, arg);
            }
        }
        arg_idx += 1;

        // min bound value
        double min_bound = 0;
        if (arg_idx < PG_NARGS()) {
            Datum arg = WinGetFuncArgCurrent(winobj, arg_idx, &isnull);
            Oid valsType = get_fn_expr_argtype(fcinfo->flinfo, arg_idx);
            min_bound = (double)get_numeric_val(valsType, arg);
            lwdebug(1, "type=%d, min bound= %f", valsType, min_bound);
        }
        arg_idx += 1;

        // scale_method
        char *scale_method = 0;
        if (arg_idx < PG_NARGS()) {
            scale_method = get_scale_method_arg(winobj, arg_idx);
        }
        arg_idx += 1;

        // dist type
        char *dist_type = 0;
        if (arg_idx < PG_NARGS()) {
            dist_type = get_distance_type_arg(winobj, arg_idx);
        }
        arg_idx += 1;

        // seed
        int seed = 123456789;
        if (arg_idx < PG_NARGS()) {
            seed = DatumGetInt32(WinGetFuncArgCurrent(winobj, arg_idx, &isnull));
        }
        arg_idx += 1;

        // cpu threads
        int cpu_threads = 6;
        if (arg_idx < PG_NARGS()) {
            cpu_threads = DatumGetInt32(WinGetFuncArgCurrent(winobj, arg_idx, &isnull));
        }
        arg_idx += 1;

        // call redcap
        int *result = redcap2_window(k, N, arrayLength, (const double**)r, (const uint8_t**)w, w_size, bound_var,
                                     min_bound, redcap_method, scale_method, dist_type, seed, cpu_threads);

        // Clean
        for (int i=0; i<N; ++i) lwfree(r[i]);
        lwfree(r);
        lwfree(w_size);
        lwfree(w);
        if (bound_var) lwfree(bound_var);

        if (result == 0) {
            elog(ERROR, "redcap: can't find clusters. Please check if the connectivity of input spatial weights "
                        "is complete.");
        }

        // Safe the result
        context->result = result;
        context->isdone = true;

        lwdebug(1, "Exit pg_redcap2_window. free_lisa() done.");
    }

    if (context->isnull)
        PG_RETURN_NULL();

    curpos = WinGetCurrentPosition(winobj);
    PG_RETURN_INT16(context->result[curpos]);
}


Datum pg_redcap3_window(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_redcap3_window);
Datum pg_redcap3_window(PG_FUNCTION_ARGS) {
    WindowObject winobj = PG_WINDOW_OBJECT();
    scc_context *context;
    int64 curpos, rowcount;

    rowcount = WinGetPartitionRowCount(winobj);
    context = (scc_context *)WinGetPartitionLocalMemory(winobj, sizeof(scc_context) + sizeof(int) * rowcount);

    if (!context->isdone) {
        bool isnull, isout;

        /* We also need a non-zero N */
        int N = (int) WinGetPartitionRowCount(winobj);
        if (N <= 0) {
            context->isdone = true;
            context->isnull = true;
            PG_RETURN_NULL();
        }

        // read data and weights
        uint8_t **w = lwalloc(sizeof(uint8_t *) * N);
        size_t *w_size = lwalloc(sizeof(size_t) * N);
        double **r =  lwalloc(sizeof(double) * N);

        // get meta-data of input Array
        ArrayType *array;
        Oid arrayElementType;
        int16 arrayElementTypeWidth; //The array element type widths (should always be 4)
        // // The array element type "is passed by value" flags (not used, should always be true):
        bool arrayElementTypeByValue;
        // The array element type alignment codes (not used):
        char arrayElementTypeAlignmentCode;
        // The array contents, as PostgreSQL "datum" objects:
        Datum *arrayContent;
        // List of "is null" flags for the array contents:
        bool *arrayNullFlags;
        // The size of each array:
        int arrayLength;

        int arg_idx_data = 0, arg_idx_weights = 1;

        for (size_t i = 0; i < N; i++) {
            Datum arg0 = WinGetFuncArgInPartition(winobj, arg_idx_data, i, WINDOW_SEEK_HEAD, false, &isnull, &isout);
            if (isnull) {
                PG_RETURN_NULL();
            }
            array = DatumGetArrayTypeP(arg0);
            if (i == 0) {
                array = DatumGetArrayTypeP(arg0);
                arrayElementType = ARR_ELEMTYPE(array);
                check_if_numeric_type(arrayElementType);
                get_typlenbyvalalign(arrayElementType, &arrayElementTypeWidth, &arrayElementTypeByValue,
                                     &arrayElementTypeAlignmentCode);
            }
            // Extract the array contents (as Datum objects).
            deconstruct_array(array, arrayElementType, arrayElementTypeWidth, arrayElementTypeByValue,
                              arrayElementTypeAlignmentCode, &arrayContent, &arrayNullFlags, &arrayLength);
            r[i] = lwalloc(sizeof(double) * arrayLength);
            for (size_t j = 0; j < arrayLength; ++j) {
                r[i][j] = get_numeric_val(arrayElementType, arrayContent[j]);
            }

            Datum arg1 = WinGetFuncArgInPartition(winobj, arg_idx_weights, i, WINDOW_SEEK_HEAD, false, &isnull, &isout);
            if (isnull) {
                PG_RETURN_NULL();
            }
            bytea *w_bytea = DatumGetByteaP(arg1); //shallow copy
            uint8_t *w_val = (uint8_t *) VARDATA(w_bytea);
            w[i] = w_val;
            w_size[i] = VARSIZE_ANY_EXHDR(w_bytea);
        }

        char* redcap_method = 0;
        VarChar *arg = (VarChar *)DatumGetVarCharPP(WinGetFuncArgCurrent(winobj, 2, &isnull));
        redcap_method = (char *)VARDATA(arg);
        if (!check_redcap_method(redcap_method)) {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("redcap method should be one of: \"firstorder-singlelinkage\", \"fullorder-completelinkage\", \"fullorder-averagelinkage\",\"fullorder-singlelinkage\", \"fullorder-wardlinkage\"")));
        }

        lwdebug(1, "pg_redcap3_window. method=%s", redcap_method);

        // read arguments
        int arg_idx = 3;

        // min bound variable
        double *bound_var  = 0;
        if (arg_idx < PG_NARGS()) {
            Oid valsType = get_fn_expr_argtype(fcinfo->flinfo, arg_idx);
            check_if_numeric_type(valsType);
            bound_var = (double*) lwalloc(sizeof(double) * N);
            for (size_t i = 0; i < N; i++) {
                Datum arg = WinGetFuncArgInPartition(winobj, arg_idx, i, WINDOW_SEEK_HEAD, false, &isnull, &isout);
                bound_var[i] = (double)get_numeric_val(valsType, arg);
            }
        }
        arg_idx += 1;

        // min bound value
        double min_bound = 0;
        if (arg_idx < PG_NARGS()) {
            Datum arg = WinGetFuncArgCurrent(winobj, arg_idx, &isnull);
            Oid valsType = get_fn_expr_argtype(fcinfo->flinfo, arg_idx);
            min_bound = (double)get_numeric_val(valsType, arg);
        }
        arg_idx += 1;


        // other optional args
        // scale_method
        char *scale_method = 0;
        if (arg_idx < PG_NARGS()) {
            scale_method = get_scale_method_arg(winobj, arg_idx);
        }
        arg_idx += 1;

        // dist type
        char *dist_type = 0;
        if (arg_idx < PG_NARGS()) {
            dist_type = get_distance_type_arg(winobj, arg_idx);
        }
        arg_idx += 1;

        // seed
        int seed = 123456789;
        if (arg_idx < PG_NARGS()) {
            seed = DatumGetInt32(WinGetFuncArgCurrent(winobj, arg_idx, &isnull));
        }
        arg_idx += 1;

        // cpu threads
        int cpu_threads = 6;
        if (arg_idx < PG_NARGS()) {
            cpu_threads = DatumGetInt32(WinGetFuncArgCurrent(winobj, arg_idx, &isnull));
        }
        arg_idx += 1;

        // call redcap
        int *result = redcap2_window(N*N, N, arrayLength, (const double**)r, (const uint8_t**)w, w_size, bound_var,
                                     min_bound, redcap_method, scale_method, dist_type, seed, cpu_threads);

        // Clean
        for (int i=0; i<N; ++i) lwfree(r[i]);
        lwfree(r);
        lwfree(w_size);
        lwfree(w);
        if (bound_var) lwfree(bound_var);

        if (result == 0) {
            elog(ERROR, "redcap: can't find clusters. Please check if the connectivity of input spatial weights "
                        "is complete.");
        }

        // Safe the result
        context->result = result;
        context->isdone = true;

        lwdebug(1, "Exit pg_redcap3_window. free_lisa() done.");
    }

    if (context->isnull)
        PG_RETURN_NULL();

    curpos = WinGetCurrentPosition(winobj);
    PG_RETURN_INT16(context->result[curpos]);
}

#ifdef __cplusplus
}
#endif