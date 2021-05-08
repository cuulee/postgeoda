/**
 * Author: Xun Li <lixun910@gmail.com>
 *
 * Changes:
 * 2021-4-28 add pg_neighbor_match_test_window()
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
#include "weights.h"
#include "lisa.h"

#ifndef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/**
 * pg_neighbor_match_test_window()
 *
 * Array[ep_pov, ep_unem], 4
 *
 * @param fcinfo
 * @return
 */
Datum pg_neighbor_match_test_window(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pg_neighbor_match_test_window);
Datum pg_neighbor_match_test_window(PG_FUNCTION_ARGS) {
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

        // meta-data of input Array
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


        // read data
        List *geoms = 0; // raw geometries
        List *fids = 0;
        double **r = lwalloc(sizeof(double) * N);

        lwdebug(0, "Init pg_neighbor_match_test_window. N=%d", N);

        for (size_t i = 0; i < N; i++) {
            Datum arg_val = WinGetFuncArgInPartition(winobj, 1, i, WINDOW_SEEK_HEAD, false, &isnull, &isout);
            if (isnull) {
                PG_RETURN_NULL();
            }
            array = DatumGetArrayTypeP(arg_val);
            if (i == 0) {
                array = DatumGetArrayTypeP(arg_val);
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

            // the_geom
            Datum arg1 = WinGetFuncArgInPartition(winobj, 0, i, WINDOW_SEEK_HEAD, false, &isnull, &isout);
            bytea *bytea_wkb = (bytea*)PG_DETOAST_DATUM(arg1);
            uint8_t *wkb = (uint8_t*)VARDATA(bytea_wkb);
            LWGEOM *lwgeom = lwgeom_from_wkb(wkb, VARSIZE_ANY_EXHDR(bytea_wkb), LW_PARSER_CHECK_ALL);
            if (i==0) {
                geoms = list_make1(lwgeom);
                fids = list_make1_int(i);
            } else {
                geoms = lappend(geoms, lwgeom);
                fids = lappend_int(fids, i);
            }
        }

        // read arguments
        int arg_index = 2;

        int k = 4;
        if (arg_index < PG_NARGS()) {
            k = DatumGetInt64(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
            if (isnull || k <= 0) {
                k = 4;
            }
        }
        arg_index += 1;

        char* scale_method = 0;
        if (arg_index < PG_NARGS()) {
            VarChar *arg = (VarChar *)DatumGetVarCharPP(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
            scale_method = (char *)VARDATA(arg);
            if (!check_scale_method(scale_method)) {
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("scaling method should be one of: 'raw', 'standardize', 'demean', 'mad', 'range_standardize', 'range_adjust'")));
            }
        }
        arg_index += 1;

        char* dist_type = 0;
        if (arg_index < PG_NARGS()) {
            VarChar *arg = (VarChar *)DatumGetVarCharPP(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
            dist_type = (char *)VARDATA(arg);
            if (!check_dist_type(dist_type)) {
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("distance type should be one of: 'euclidean', 'manhattan'")));
            }
        }
        arg_index += 1;

        // power
        double power = 1.0;
        if (arg_index < PG_NARGS()) {
            power = DatumGetFloat4(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
            if (isnull || power <= 0) {
                power = 0.05;
            }
        }
        arg_index += 1;

        bool is_inverse = false;
        if (arg_index < PG_NARGS()) {
            is_inverse = DatumGetBool(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
        }
        arg_index += 1;

        bool is_arc = false;
        if (arg_index < PG_NARGS()) {
            is_arc = DatumGetBool(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
        }
        arg_index += 1;

        bool is_mile = true;
        if (arg_index < PG_NARGS()) {
            is_mile = DatumGetBool(WinGetFuncArgCurrent(winobj, arg_index, &isnull));
        }
        arg_index += 1;


        double **result = neighbor_match_test_window(fids, geoms, k, arrayLength, N, (const double**)r,
                                                     power, is_inverse, is_arc, is_mile, scale_method, dist_type);

        // Safe the result
        context->result = result;
        context->isdone = true;

        // clean
        lwdebug(1, "Clean pg_neighbor_match_test_window.");
        for (int i=0; i<N; ++i) lwfree(r[i]);
        lwfree(r);

        lwdebug(1, "Exit pg_neighbor_match_test_window.");
    }

    if (context->isnull)
        PG_RETURN_NULL();

    curpos = WinGetCurrentPosition(winobj);

    // Wrap the results in a new PostgreSQL array object.
    double *p = context->result[curpos];
    Datum elems[2];
    elems[0] = Float8GetDatum(p[0]); // double to Datum
    elems[1] = Float8GetDatum(p[1]);
    free(p);

    int nelems = 2;
    Oid elmtype = FLOAT8OID;
    int16 elmlen;
    bool elmbyval;
    char elmalign;
    get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);
    if (elems[1] == -1) { // NA probability value
        elmbyval = false;
    }
    ArrayType *array = construct_array(elems, nelems, elmtype, elmlen, elmbyval, elmalign);

    PG_RETURN_ARRAYTYPE_P(array);
}

#ifdef __cplusplus
}
#endif

