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

/**
* The binary format of spatial weights (PGWeight)
*
* char (1 bytes): weights type: 'a'->GAL 'w'->GWT
* uint32 (4 bytes): number of observations: N
* ...
* uint32 (4 bytes): index of i-th observation
* uint16 (2 bytes): number of neighbors of i-th observation (nn)
* uint32 (4 bytes x nn): neighbor id
* float (4 bytes x nn): weights value of each neighbor
* ...
*
* total size of GAL weights = 1 + 4 + (2 + nn *(4+4)) * N
* total size of GWT weights = 1 + 4 + (2 + nn * 4) * N
*
* e.g. 10 million observations, on average, each observation has nn neighbors:
* if nn=20, gal weights, total size = 0.76GB
* if nn=20, gwt weights, total size = 1.5GB
*
* e.g. 100 million observations, on average, each observation has nn neighbors:
* if nn=20, gal weights, total size = 7.6GB
* if nn=20, gwt weights, total size = 15.08GB
*
*/

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
PGWeight* create_cont_weights(List *fids, List *geoms, bool is_queen, int order, bool inc_lower,
                              double precision_threshold);

/**
 * knn weights functions bridging PG and libgeoda::knn_weights
 *
 * @param fids
 * @param geoms
 * @param k
 * @param power
 * @param is_inverse
 * @param is_arc
 * @param is_mile
 * @return
 */
PGWeight* create_knn_weights(List *fids, List *geoms, int k, double power,
                             bool is_inverse, bool is_arc, bool is_mile);

/**
 *
 * @param fids
 * @param geoms
 * @param k
 * @param power
 * @param is_inverse
 * @param is_arc
 * @param is_mile
 * @param kernel
 * @param bandwidth
 * @param adaptive_bandwidth
 * @param use_kernel_diagonal
 * @return
 */
PGWeight* create_kernel_knn_weights(List *fids, List *geoms, int k, double power,
                                    bool is_inverse, bool is_arc,
                                    bool is_mile, const char* kernel,
                                    double bandwidth, bool adaptive_bandwidth,
                                    bool use_kernel_diagonal);

/**
 *
 * @param fids
 * @param geoms
 * @param dist_threshold
 * @param power
 * @param is_inverse
 * @param is_arc
 * @param is_mile
 * @param kernel
 * @param use_kernel_diagonal
 * @return
 */
PGWeight* create_kernel_weights(List *fids, List *geoms, double dist_threshold,
                                double power, bool is_inverse, bool is_arc,
                                bool is_mile, const char* kernel,
                                bool use_kernel_diagonal);
 /**
  *
  * distance weights functions bridging PG and libgeoda::distance_weights
  *
  * @param fids
  * @param geoms
  * @param threshold
  * @param power
  * @param is_inverse
  * @param is_arc
  * @param is_mile
  * @return
  */
PGWeight* create_distance_weights(List *fids, List *geoms, double threshold,
                                  double power, bool is_inverse,
                                  bool is_arc, bool is_mile);

/**
 * get_min_distthreshold()
 *
 * This function computes the minimum pairwise distance among the observations.
 *
 * @param lfids
 * @param lwgeoms
 * @param is_arc
 * @param is_mile
 * @return
 */
double get_min_distthreshold(List *lfids, List *lwgeoms, bool is_arc, bool is_mile);

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