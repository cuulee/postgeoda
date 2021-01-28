/**
 * Author: Xun Li <lixun910@gmail.com>
 *
 * Changes:
 * 2021-1-27 Update to use libgeoda 0.0.6; Abstract it for all different lisa functions
 */

#ifndef GEODA_LISA_H
#define GEODA_LISA_H

#include <catalog/pg_type.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    bool	isdone;
    bool	isnull;
    Point  **result;
    /* variable length */
} lisa_context;

static inline void check_if_numeric_type(Oid  valsType)
{
    if (valsType != INT2OID &&
        valsType != INT4OID &&
        valsType != INT8OID &&
        valsType != FLOAT4OID &&
        valsType != FLOAT8OID) {
        ereport(ERROR, (errmsg("localmoran first parameter must be SMALLINT, INTEGER, BIGINT, REAL, or DOUBLE "
                               "PRECISION values")));
    }
}

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
            break;
    }
    return val;
}


#ifdef __cplusplus
}
#endif

#endif //GEODA_LOCALMORAN_H
