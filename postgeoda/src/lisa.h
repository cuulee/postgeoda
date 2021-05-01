/**
 * Author: Xun Li <lixun910@gmail.com>
 *
 * Changes:
 * 2021-1-27 Update to use libgeoda 0.0.6; Abstract it for all different lisa functions
 * 2021-4-28 add check_scale_method(), check_scale_method()
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
    bool	isdone;
    bool	isnull;
    int     *result;
    /* variable length */
} scc_context;

/**
 * lisa_context
 *
 * Data structure used in LISA Windows functions to return results
 */
typedef struct {
    bool	isdone;
    bool	isnull;
    double   **result;
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
        valsType != FLOAT4OID &&
        valsType != FLOAT8OID) {
        ereport(ERROR, (errmsg("The first input argument of LISA function should be values of SMALLINT, INTEGER, "
                               "BIGINT, REAL, or DOUBLE PRECISION")));
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
            ereport(ERROR, (errmsg("input values must be SMALLINT, INTEGER, BIGINT, REAL, or DOUBLE PRECISION")));
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

static inline bool check_scale_method(const char* method) {
    if (method == 0) {
        return false;
    }

    if (strncmp(method, "raw", 3) == 0) {
        return true;
    } else if (strncmp(method, "standardize", 11) == 0) {
        return true;
    } else if (strncmp(method, "demean", 5) == 0) {
        return true;
    } else if (strncmp(method, "mad", 3) == 0) {
        return true;
    } else if (strncmp(method, "range_standardize", 17) == 0) {
        return true;
    } else if (strncmp(method, "range_adjust", 12) == 0) {
        return true;
    }

    return false;
}

static inline bool check_dist_type(const char* typ) {
    if (typ == 0) {
        return false;
    }

    if (strncmp(typ, "euclidean", 9) == 0) {
        return true;
    } else if (strncmp(typ, "manhattan", 9) == 0) {
        return true;
    }

    return false;
}


static inline char* get_scale_method_arg(WindowObject winobj, int arg_index)
{
    bool isnull;
    VarChar *arg = (VarChar *)DatumGetVarCharPP(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
    if (isnull) {
        return 0;
    }
    char *scale_method = (char *)VARDATA(arg);
    if (!check_scale_method(scale_method)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("scaling method should be one of: 'raw', 'standardize', 'demean', 'mad', 'range_standardize', 'range_adjust'")));
    }
    return scale_method;
}

static inline char* get_distance_type_arg(WindowObject winobj, int arg_index)
{
    bool isnull;
    VarChar *arg = (VarChar *)DatumGetVarCharPP(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
    if (isnull) {
        return 0;
    }
    char *dist_type = (char *)VARDATA(arg);
    if (!check_dist_type(dist_type)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("distance type should be one of: 'euclidean', 'manhattan'")));
    }
    return dist_type;
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
