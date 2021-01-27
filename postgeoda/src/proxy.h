#ifndef __POST_PROXY__
#define __POST_PROXY__

#ifdef __cplusplus
extern "C" {
#endif

#include <postgres.h>
#include <utils/lsyscache.h> /* for get_typlenbyvalalign */
#include <utils/geo_decls.h> /* for Point */

typedef struct PGNeighbor
{
    uint32_t idx;
    uint16_t num_nbrs;
    uint32_t *nbrId;
    float *nbrWeight;
} PGNeighbor;

void free_pgneighbor(PGNeighbor *neighbor);

typedef struct PGWeight
{
    char w_type;
    uint32_t num_obs;
    PGNeighbor* neighbors;
} PGWeight;

void free_pgweight(PGWeight *w);



PGWeight* create_queen_weights(List *fids, List *geoms, int order, bool inc_lower, double precision_threshold);

PGWeight* create_knn_weights(List *fids, List *geoms, int k);

typedef struct PGLISA
{
    int32_t n;
    double *indicators;
    double *pvalues;
} PGLISA;

void free_pglisa(PGLISA *lisa);

Point** pg_local_moran(int N, const int64* fids, const double* r, const uint8_t* bw);

Point** pg_local_moran_warray(int N, const double* r, const uint8_t** bw, const size_t* w_size);

Point* pg_local_moran_fast(double val, const uint8_t* bw, size_t bw_size, int num_obs, const double* arr,
                           int permutations, int rnd_seed);

#ifdef __cplusplus
}
#endif

#endif