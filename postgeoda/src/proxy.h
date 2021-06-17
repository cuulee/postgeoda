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

PGWeight* create_knn_weights_sub(List *fids, List *geoms, int k, int start, int end, double power,
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
 * local_moran_window_bytea()
 *
 * The local moran function used for Window SQL function local_moran_b(), which needs
 * to take the spatial weights as a whole as an input parameter.
 *
 * This will be depreciated.
 * @param N
 * @param fids
 * @param r
 * @param bw
 * @return  double**
 */
double** local_moran_window_bytea(int N, const int64* fids, const double* r, const uint8_t* bw);

/**
 * local_moran_window()
 *
 * The local moran function used for Window SQL function local_moran()
 * @param N
 * @param r
 * @param bw
 * @param w_size
 * @return double**
 */
double** local_moran_window(int N, const double* r, const uint8_t** bw, const size_t* w_size, int permutations,
                           char *method, double significance_cutoff, int cpu_threads, int seed);

double** local_moran_fast(int N, int NN, const double* r, const double* arr, const uint8_t** bw, const size_t* w_size, int permutations,
                          char *method, double significance_cutoff, int cpu_threads, int seed);


double** local_joincount_window(int N, const double* r, const uint8_t** bw, const size_t* w_size, int permutations,
                                char *method, double significance_cutoff, int cpu_threads, int seed);

double** local_joincount_fast(int N, int NN, const double* r, const double* arr, const uint8_t** bw, const size_t* w_size, int permutations,
                          char *method, double significance_cutoff, int cpu_threads, int seed);

double** local_bijoincount_window(int N, const double* r1, const double* r2, const uint8_t** bw, const size_t* w_size,
                                  int permutations, char *method, double significance_cutoff, int cpu_threads,
                                  int seed);

double** local_multijoincount_window(int N, int n_vars, const double** r, const uint8_t** bw, const size_t* w_size,
                                     int permutations, char *method, double significance_cutoff, int cpu_threads,
                                     int seed);
/**
 *
 * @param N
 * @param r
 * @param bw
 * @param w_size
 * @param permutations
 * @param method
 * @param significance_cutoff
 * @param cpu_threads
 * @param seed
 * @return
 */
double** local_g_window(int N, const double* r, const uint8_t** bw, const size_t* w_size, int permutations,
                        char *method, double significance_cutoff, int cpu_threads, int seed);

/**
 *
 * @param N
 * @param r
 * @param bw
 * @param w_size
 * @param permutations
 * @param method
 * @param significance_cutoff
 * @param cpu_threads
 * @param seed
 * @return
 */
double** local_gstar_window(int N, const double* r, const uint8_t** bw, const size_t* w_size, int permutations,
                            char *method, double significance_cutoff, int cpu_threads, int seed);

double** local_geary_window(int N, const double* r, const uint8_t** bw, const size_t* w_size, int permutations,
                            char *method, double significance_cutoff, int cpu_threads, int seed);

double** local_multigeary_window(int n_vars, int N, const double** r, const uint8_t** bw,
                                        const size_t* w_size, int permutations, char *method,
                                        double significance_cutoff, int cpu_threads, int seed);

double** local_quantilelisa_window(int k, int quantile, int N, const double* r, const uint8_t** bw,
                                   const size_t* w_size, int permutations, char *method, double significance_cutoff,
                                   int cpu_threads, int seed);

double** local_multiquantilelisa_window(int n_vars, int* k, int* quantile, int N, const double** r, const uint8_t** bw,
                                        const size_t* w_size, int permutations, char *method,
                                        double significance_cutoff, int cpu_threads, int seed);

double** neighbor_match_test_window(List *lfids, List *lwgeoms, int k, int n_vars, int N, const double** r,
                                    double power, bool is_inverse, bool is_arc, bool is_mile,
                                    const char *scale_method, const char* dist_type);

double* excess_risk_window(int num_obs, double* e, double* b);

double* eb_rate_window(int num_obs, double* e, double* b);

double* spatial_lag_window(int N, const double* r, const uint8_t** bw, const size_t* w_size, bool is_binary,
                           bool row_stand, bool inc_diag);

double* spatial_rate_window(int N, double* e, double* b, const uint8_t** bw, const size_t* w_size);

double* spatial_eb_window(int N, double* e, double* b, const uint8_t** bw, const size_t* w_size);

double* pg_hinge_aggregate(List *data, List *undefs, bool is_hinge15);

double* pg_percentile_aggregate(List *data, List *undefs, int *n_breaks);

double* pg_stddev_aggregate(List *data, List *undefs, int *n_breaks);

double* pg_quantile_aggregate(List *data, List *undefs, int k);

double* pg_naturalbreaks_aggregate(List *data, List *undefs, int k);

int* redcap1_window(int k, int N, int n_vars, const double** r, const uint8_t** bw, const size_t* w_size,
                    int min_region, const char* redcap_method, const char *scale_type, const char* dist_type,
                    int seed, int cpu_threads);

int* redcap2_window(int k, int N, int n_vars, const double** r, const uint8_t** bw, const size_t* w_size,
                   const double* bound_var, double min_bound, const char* redcap_method, const char *scale_type,
                   const char* dist_type, int seed, int cpu_threads);

#ifdef __cplusplus
}
#endif

#endif