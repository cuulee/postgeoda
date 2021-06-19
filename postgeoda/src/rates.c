/**
 * Author: Xun Li <lixun910@gmail.com>
 *
 * Changes:
 * 2021-5-7 add pg_excess_risk()
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
 * rates_context
 *
 * Data structure used in LISA Windows functions to return results
 */
typedef struct {
    bool	isdone;
    bool	isnull;
    double   *result;
    /* variable length */
} rates_context;

Datum pg_excess_risk(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_excess_risk);
Datum pg_excess_risk(PG_FUNCTION_ARGS) {
    WindowObject winobj = PG_WINDOW_OBJECT();
    rates_context *context;
    int64 curpos, rowcount;

    Oid valsType = get_fn_expr_argtype(fcinfo->flinfo, 0);
    check_if_numeric_type(valsType);

    Oid valsType1 = get_fn_expr_argtype(fcinfo->flinfo, 1);
    check_if_numeric_type(valsType1);

    rowcount = WinGetPartitionRowCount(winobj);
    context = (rates_context *)WinGetPartitionLocalMemory(winobj,sizeof(rates_context) + sizeof(int) * rowcount);

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
        double *e = lwalloc(sizeof(double) * N);
        double *b = lwalloc(sizeof(double) * N);

        //lwdebug(1, "Init pg_excess_risk. N=%d", N);

        for (size_t i = 0; i < N; i++) {
            Datum arg = WinGetFuncArgInPartition(winobj, 0, i,
                                                 WINDOW_SEEK_HEAD, false, &isnull, &isout);
            e[i] = get_numeric_val(valsType, arg);
            Datum arg1 = WinGetFuncArgInPartition(winobj, 1, i,
                                                  WINDOW_SEEK_HEAD, false, &isnull, &isout);
            b[i] = get_numeric_val(valsType1, arg1);
        }


        // compute lisa
        lwdebug(1, "Enter pg_excess_risk. N=%d", N);
        double *result = excess_risk_window(N, e, b);

        // Safe the result
        context->result = result;
        context->isdone = true;

        // Clean
        lwfree(e);
        lwfree(b);

        lwdebug(1, "Exit pg_excess_risk.");
    }

    if (context->isnull) {
        PG_RETURN_NULL();
    }

    curpos = WinGetCurrentPosition(winobj);

    PG_RETURN_FLOAT8(context->result[curpos]);
}

Datum pg_eb_rate(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_eb_rate);
Datum pg_eb_rate(PG_FUNCTION_ARGS) {
    WindowObject winobj = PG_WINDOW_OBJECT();
    rates_context *context;
    int64 curpos, rowcount;

    Oid valsType = get_fn_expr_argtype(fcinfo->flinfo, 0);
    check_if_numeric_type(valsType);

    Oid valsType1 = get_fn_expr_argtype(fcinfo->flinfo, 1);
    check_if_numeric_type(valsType1);

    rowcount = WinGetPartitionRowCount(winobj);
    context = (rates_context *)WinGetPartitionLocalMemory(winobj,sizeof(rates_context) + sizeof(int) * rowcount);

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
        double *e = lwalloc(sizeof(double) * N);
        double *b = lwalloc(sizeof(double) * N);

        //lwdebug(1, "Init pg_excess_risk. N=%d", N);

        for (size_t i = 0; i < N; i++) {
            Datum arg = WinGetFuncArgInPartition(winobj, 0, i,
                                                 WINDOW_SEEK_HEAD, false, &isnull, &isout);
            e[i] = get_numeric_val(valsType, arg);
            Datum arg1 = WinGetFuncArgInPartition(winobj, 1, i,
                                                  WINDOW_SEEK_HEAD, false, &isnull, &isout);
            b[i] = get_numeric_val(valsType1, arg1);
        }


        // compute lisa
        lwdebug(1, "Enter pg_eb_rate. N=%d", N);
        double *result = eb_rate_window(N, e, b);

        // Safe the result
        context->result = result;
        context->isdone = true;

        // Clean
        lwfree(e);
        lwfree(b);

        lwdebug(1, "Exit pg_eb_rate.");
    }

    if (context->isnull) {
        PG_RETURN_NULL();
    }

    curpos = WinGetCurrentPosition(winobj);

    PG_RETURN_FLOAT8(context->result[curpos]);
}


Datum pg_spatial_lag(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_spatial_lag);
Datum pg_spatial_lag(PG_FUNCTION_ARGS) {
    WindowObject winobj = PG_WINDOW_OBJECT();
    rates_context *context;
    int64 curpos, rowcount;

    Oid valsType = get_fn_expr_argtype(fcinfo->flinfo, 0);
    check_if_numeric_type(valsType);

    rowcount = WinGetPartitionRowCount(winobj);
    context = (rates_context *)WinGetPartitionLocalMemory(winobj,sizeof(rates_context) + sizeof(int) * rowcount);

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

        //lwdebug(1, "Init pg_spatial_lag. N=%d", N);

        for (size_t i = 0; i < N; i++) {
            Datum arg = WinGetFuncArgInPartition(winobj, 0, i,
                                                 WINDOW_SEEK_HEAD, false, &isnull, &isout);
            r[i] = get_numeric_val(valsType, arg);

            Datum arg1 = WinGetFuncArgInPartition(winobj, 1, i,
                                                  WINDOW_SEEK_HEAD, false, &isnull, &isout);
            bytea *w_bytea = DatumGetByteaP(arg1); //shallow copy
            uint8_t *w_val = (uint8_t *) VARDATA(w_bytea);
            w[i] = w_val;
            w_size[i] = VARSIZE_ANY_EXHDR(w_bytea);
        }

        int arg_index = 2;

        bool is_binary = true;
        if (arg_index < PG_NARGS()) {
            is_binary = DatumGetBool(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
        }
        arg_index += 1;


        bool row_standardize = true;
        if (arg_index < PG_NARGS()) {
            row_standardize = DatumGetBool(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
        }
        arg_index += 1;

        bool include_diagonal = false;
        if (arg_index < PG_NARGS()) {
            include_diagonal = DatumGetBool(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
        }
        arg_index += 1;

        // compute lisa
        lwdebug(1, "Enter pg_spatial_lag. N=%d", N);
        double *result = spatial_lag_window(N, r, (const uint8_t**)w, w_size, is_binary, row_standardize,
                include_diagonal);

        // Safe the result
        context->result = result;
        context->isdone = true;

        // Clean
        lwfree(r);
        lwfree(w_size);
        lwfree(w);

        lwdebug(1, "Exit pg_spatial_lag.");
    }

    if (context->isnull) {
        PG_RETURN_NULL();
    }

    curpos = WinGetCurrentPosition(winobj);

    PG_RETURN_FLOAT8(context->result[curpos]);
}


Datum pg_spatial_rate(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_spatial_rate);
Datum pg_spatial_rate(PG_FUNCTION_ARGS) {
    WindowObject winobj = PG_WINDOW_OBJECT();
    rates_context *context;
    int64 curpos, rowcount;

    Oid valsType = get_fn_expr_argtype(fcinfo->flinfo, 0);
    check_if_numeric_type(valsType);

    Oid valsType1 = get_fn_expr_argtype(fcinfo->flinfo, 1);
    check_if_numeric_type(valsType1);

    rowcount = WinGetPartitionRowCount(winobj);
    context = (rates_context *)WinGetPartitionLocalMemory(winobj,sizeof(rates_context) + sizeof(int) * rowcount);

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
        double *e = lwalloc(sizeof(double) * N);
        double *b = lwalloc(sizeof(double) * N);
        uint8_t **w = lwalloc(sizeof(uint8_t *) * N);
        size_t *w_size = lwalloc(sizeof(size_t) * N);

        //lwdebug(1, "Init pg_spatial_rate. N=%d", N);

        for (size_t i = 0; i < N; i++) {
            Datum arg = WinGetFuncArgInPartition(winobj, 0, i,
                                                 WINDOW_SEEK_HEAD, false, &isnull, &isout);
            e[i] = get_numeric_val(valsType, arg);
            Datum arg1 = WinGetFuncArgInPartition(winobj, 1, i,
                                                  WINDOW_SEEK_HEAD, false, &isnull, &isout);
            b[i] = get_numeric_val(valsType1, arg1);
            Datum arg2 = WinGetFuncArgInPartition(winobj, 2, i,
                                                  WINDOW_SEEK_HEAD, false, &isnull, &isout);
            bytea *w_bytea = DatumGetByteaP(arg2); //shallow copy
            uint8_t *w_val = (uint8_t *) VARDATA(w_bytea);
            w[i] = w_val;
            w_size[i] = VARSIZE_ANY_EXHDR(w_bytea);
        }


        // compute lisa
        lwdebug(1, "Enter pg_spatial_rate. N=%d", N);
        double *result = spatial_rate_window(N, e, b, (const uint8_t**)w, w_size);

        // Safe the result
        context->result = result;
        context->isdone = true;

        // Clean
        lwfree(e);
        lwfree(b);
        lwfree(w_size);
        lwfree(w);

        lwdebug(1, "Exit pg_spatial_rate.");
    }

    if (context->isnull) {
        PG_RETURN_NULL();
    }

    curpos = WinGetCurrentPosition(winobj);

    PG_RETURN_FLOAT8(context->result[curpos]);
}


Datum pg_spatial_eb(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_spatial_eb);
Datum pg_spatial_eb(PG_FUNCTION_ARGS) {
    WindowObject winobj = PG_WINDOW_OBJECT();
    rates_context *context;
    int64 curpos, rowcount;

    Oid valsType = get_fn_expr_argtype(fcinfo->flinfo, 0);
    check_if_numeric_type(valsType);

    Oid valsType1 = get_fn_expr_argtype(fcinfo->flinfo, 1);
    check_if_numeric_type(valsType1);

    rowcount = WinGetPartitionRowCount(winobj);
    context = (rates_context *)WinGetPartitionLocalMemory(winobj,sizeof(rates_context) + sizeof(int) * rowcount);

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
        double *e = lwalloc(sizeof(double) * N);
        double *b = lwalloc(sizeof(double) * N);
        uint8_t **w = lwalloc(sizeof(uint8_t *) * N);
        size_t *w_size = lwalloc(sizeof(size_t) * N);

        //lwdebug(1, "Init pg_spatial_eb. N=%d", N);

        for (size_t i = 0; i < N; i++) {
            Datum arg = WinGetFuncArgInPartition(winobj, 0, i,
                                                 WINDOW_SEEK_HEAD, false, &isnull, &isout);
            e[i] = get_numeric_val(valsType, arg);
            Datum arg1 = WinGetFuncArgInPartition(winobj, 1, i,
                                                  WINDOW_SEEK_HEAD, false, &isnull, &isout);
            b[i] = get_numeric_val(valsType1, arg1);
            Datum arg2 = WinGetFuncArgInPartition(winobj, 2, i,
                                                  WINDOW_SEEK_HEAD, false, &isnull, &isout);
            bytea *w_bytea = DatumGetByteaP(arg2); //shallow copy
            uint8_t *w_val = (uint8_t *) VARDATA(w_bytea);
            w[i] = w_val;
            w_size[i] = VARSIZE_ANY_EXHDR(w_bytea);
        }


        // compute lisa
        lwdebug(1, "Enter pg_spatial_eb. N=%d", N);
        double *result = spatial_eb_window(N, e, b, (const uint8_t**)w, w_size);

        // Safe the result
        context->result = result;
        context->isdone = true;

        // Clean
        lwfree(e);
        lwfree(b);
        lwfree(w_size);
        lwfree(w);

        lwdebug(1, "Exit pg_spatial_eb.");
    }

    if (context->isnull) {
        PG_RETURN_NULL();
    }

    curpos = WinGetCurrentPosition(winobj);

    PG_RETURN_FLOAT8(context->result[curpos]);
}


#ifdef __cplusplus
}
#endif

