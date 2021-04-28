/**
 * Author: Xun Li <lixun910@gmail.com>
 *
 * Changes:
 * 2021-1-27 Update to use libgeoda 0.0.6; Abstract it for all different lisa functions
 */

#ifndef GEODA_LISA_H
#define GEODA_LISA_H

#ifdef __cplusplus
extern "C" {
#endif

#include <catalog/pg_type.h>
#include <fmgr.h>
#include <utils/numeric.h>
#include <pgtypes_numeric.h>
#include <windowapi.h>

typedef struct {
    float lisa;
    float pvalue;
    int category;
} lisa_result;

/**
 * lisa_context
 *
 * Data structure used in LISA Windows functions to return results
 */
typedef struct {
    bool	isdone;
    bool	isnull;
    Point  **result;
    //Datum  **output;
    /* variable length */
} lisa_context;

/**
 * check_if_numeric_type()
 *
 * Check if input argument is numeric type
 * @param valsType
 */
static inline void check_if_numeric_type(Oid  valsType)
{
    if (valsType != INT2OID &&
        valsType != INT4OID &&
        valsType != INT8OID &&
        valsType != NUMERICOID &&
        valsType != FLOAT4OID &&
        valsType != FLOAT8OID) {
        ereport(ERROR, (errmsg("first parameter of LISA function must be SMALLINT, INTEGER, BIGINT, REAL, or DOUBLE "
                               "PRECISION values")));
    }
}

/**
 * get_numeric_val()
 *
 * Get the numeric value of an input argument
 *
 * {"DECIMAL", 1700},      ALSO KNOWN  AS NUMERIC
 *
 * @param valsType
 * @param arg
 * @return
 */
static inline double get_numeric_val(Oid valsType, Datum arg)
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
            ereport(ERROR, (errmsg("LISA input values must be SMALLINT, INTEGER, BIGINT, REAL, or DOUBLE PRECISION values")));
            break;
    }
    return val;
}

/**
 * check_perm_method
 *
 * Check if permutation method is one of: complete or lookup
 *
 * @param method
 * @return
 */
static inline bool check_perm_method(const char* method) {
    if (method == 0) {
        return false;
    }

    if (strncmp(method, "complete", 8) == 0) {
        return true;
    } else if (strncmp(method, "lookup", 6) == 0) {
        return true;
    }

    return false;
}

typedef struct {
    int permutations;
    char *method;
    double significance_cutoff;
    int cpu_threads;
    int seed;
} lisa_arguments;

static inline void read_lisa_arguments(int arg_index, int pg_nargs, WindowObject winobj, lisa_arguments *args) {

    bool isnull;

    args->permutations = 999;
    if (arg_index < pg_nargs) {
        args->permutations = DatumGetInt16(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
        if (isnull || args->permutations <= 0) {
            args->permutations = 999;
        }
    }
    arg_index += 1;

    if (arg_index < pg_nargs) {
        VarChar *arg = (VarChar *)DatumGetVarCharPP(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
        args->method = (char *)VARDATA(arg);
        if (!check_perm_method(args->method)) {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("Permutation method has to be one of: complete, lookup")));
        }
    }
    arg_index += 1;

    if (arg_index < pg_nargs) {
        args->significance_cutoff = DatumGetFloat4(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
        if (isnull || args->significance_cutoff <= 0) {
            args->significance_cutoff = 0.05;
        }
    }
    arg_index += 1;

    if (arg_index < pg_nargs) {
        args->cpu_threads = DatumGetInt16(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
        if (isnull || args->cpu_threads <= 0) {
            args->cpu_threads = 6;
        }
    }
    arg_index += 1;

    if (arg_index < pg_nargs) {
        args->seed = DatumGetInt16(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
        if (isnull || args->seed <= 0) {
            args->seed = 123456789;
        }
    }
    arg_index += 1;
}

#ifdef __cplusplus
}
#endif

#endif //GEODA_LOCALMORAN_H
