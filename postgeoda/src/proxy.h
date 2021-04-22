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

/**
 * Contiguity (queen/rook) weights functions bridging PG and libgeoda
 *
 * @param fids
 * @param geoms
 * @param is_queen
 * @param order
 * @param inc_lower
 * @param precision_threshold
 * @return
 */
PGWeight* create_cont_weights(List *fids, List *geoms, bool is_queen, int order, bool inc_lower, double precision_threshold);

/**
 * knn weights functions bridging PG and libgeoda::knn_weights
 *
 * @param fids
 * @param geoms
 * @param k
 * @return
 */
PGWeight* create_knn_weights(List *fids, List *geoms, int k);

/**
 * distance weights functions bridging PG and libgeoda::distance_weights
 *
 * @param fids
 * @param geoms
 * @param threshold
 * @return
 */
PGWeight* create_distance_weights(List *fids, List *geoms, double threshold);

/**
 * Structure to exchange lisa data between PG and libgeoda
 */
typedef struct PGLISA
{
    int32_t n;
    double *indicators; // cluster indicators
    double *pvalues; // pseudo p-values
} PGLISA;

/**
 * function to free PGLISA object
 *
 * @param lisa
 */
void free_pglisa(PGLISA *lisa);

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

/**
 * briding PG to libgeoda::local_bijoincount
 * @param N
 * @param fids
 * @param r1
 * @param r2
 * @param bw
 * @return
 */
Point** pg_bivariate_local_joincount(int N, const int64* fids, const double* r1, const double* r2, const uint8_t* bw);

/**
 * bridging PG to libgeoda::local_g
 * @param N
 * @param fids
 * @param r
 * @param bw
 * @return
 */
Point** pg_local_g(int N, const int64* fids, const double* r, const uint8_t* bw);

/**
 * bridging PG to libgeoda::local_start
 *
 * @param N
 * @param fids
 * @param r
 * @param bw
 * @return
 */
Point** pg_local_gstar(int N, const int64* fids, const double* r, const uint8_t* bw);

/**
 * briding PG to libgeoda::gda_quantilelisa()
 *
 * @param k
 * @param quantile
 * @param N the number of objects
 * @param fids the feature_id (fids) of query objects
 * @param r real values
 * @param bw Binary weights
 * @return
 */
Point** pg_quantilelisa(int k, int quantile, int N, const double* r, const uint8_t** bw, const size_t* w_size);

#ifdef __cplusplus
}
#endif

#endif