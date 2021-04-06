/**
 * Author: Xun Li <lixun910@gmail.com>
 *
 * Changes:
 * 2021-1-27 Update to use libgeoda 0.0.6; Add pg_local_joincount()
 */

#ifndef __POST_PROXY__
#define __POST_PROXY__

#ifdef __cplusplus
extern "C" {
#endif

#include <postgres.h>
#include <utils/lsyscache.h> /* for get_typlenbyvalalign */
#include <utils/geo_decls.h> /* for Point */

// Structure to exchange weights data between PG and libgeoda
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

// Weights functions bridging PG and libgeoda
PGWeight* create_queen_weights(List *fids, List *geoms, int order, bool inc_lower, double precision_threshold);

PGWeight* create_knn_weights(List *fids, List *geoms, int k);

PGWeight* create_distance_weights(List *fids, List *geoms, double threshold);

// Structure to exchange lisa data between PG and libgeoda
typedef struct PGLISA
{
    int32_t n;
    double *indicators; // cluster indicators
    double *pvalues; // pseudo p-values
} PGLISA;

void free_pglisa(PGLISA *lisa);

// LISA functions bridging PG and libgeoda
/**
 *
 * @param N
 * @param fids
 * @param r
 * @param bw
 * @return  Point** NOTE: Point is used to store (indicator, pvalue) as a pair returned for each row
 */
Point** pg_local_moran(int N, const int64* fids, const double* r, const uint8_t* bw);

Point** pg_local_moran_warray(int N, const double* r, const uint8_t** bw, const size_t* w_size);

Point* pg_local_moran_fast(double val, const uint8_t* bw, size_t bw_size, int num_obs, const double* arr,
                           int permutations, int rnd_seed);


/**
 * bridging PG to libgeoda::local_joincount
 * @param N
 * @param fids
 * @param r
 * @param bw
 * @return
 */
Point** pg_local_joincount(int N, const int64* fids, const double* r, const uint8_t* bw);

Point** pg_bivariate_local_joincount(int N, const int64* fids, const double* r1, const double* r2, const uint8_t* bw);
/**
 * briding PG to libgeoda::local_g
 * @param N
 * @param fids
 * @param r
 * @param bw
 * @return
 */
Point** pg_local_g(int N, const int64* fids, const double* r, const uint8_t* bw);

#ifdef __cplusplus
}
#endif

#endif