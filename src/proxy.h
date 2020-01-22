#ifndef __POST_PROXY__
#define __POST_PROXY__

#include <postgres.h>
#include <utils/lsyscache.h> /* for get_typlenbyvalalign */

#ifdef __cplusplus
extern "C" {
#endif

typedef struct PGWeight
{
    int32_t* nbrId;
    double* nbrWeight;
} PGWeight;

PGWeight* create_queen_weights(Oid geomOid, List *geoms);

#ifdef __cplusplus
}
#endif

#endif