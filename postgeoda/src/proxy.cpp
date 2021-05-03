/**
 * Author: Xun Li <lixun910@gmail.com>
 *
 * Changes:
 * 2021-1-27 Update to use libgeoda 0.0.6;
 * 2021-4-23 Update create_knn_weights(); add create_kernel_weights();
 * add create_kernel_knn_weights();
 * 2021-4-28 add neighbor_match_test_window()
 * 2021-4-29 add pg_hinge15_aggregate()
 */

#include <vector>

#include <libgeoda/gda_sa.h>
#include <libgeoda/sa/LISA.h>
#include <libgeoda/weights/GalWeight.h>
#include <libgeoda/gda_weights.h>
#include <libgeoda/GeoDaSet.h>
#include <libgeoda/GenUtils.h>
#include <libgeoda/pg/geoms.h>
#include <libgeoda/pg/utils.h>
#include <libgeoda/gda_data.h>

#include "binweight.h"
#include "postgeoda.h"
#include "proxy.h"
#include "lisa.h"

void free_pgneighbor(PGNeighbor *neighbor, char w_type)
{
    if (neighbor)    {
        free(neighbor->nbrId);
        if (w_type == 'w') free(neighbor->nbrWeight);
    }
}

void free_pgweight(PGWeight *w)
{
    if (w) {
        for (size_t i=0; i < w->num_obs; ++i) {
            free_pgneighbor(&w->neighbors[i], w->w_type);
        }
        free(w);
    }
}

/**
 * Create geoda instance from the_geom and fids of the table
 *
 * @param lfids
 * @param lwgeoms
 * @return
 */
PostGeoDa* build_pg_geoda(List *lfids, List *lwgeoms) {
    lwdebug(1, "Enter build_pg_geoda.");

    ListCell *l;
    size_t nelems = list_length(lwgeoms);
    bool b_maptype = false;
    size_t count = 0;

    std::vector<uint32_t> fids(nelems, 0);
    foreach(l, lfids) {
        fids[count] = lfirst_int(l);
        lwdebug(5, "build_pg_geoda fids=%d", fids[count]);
        count++;
    }

    lwdebug(1, "build_pg_geoda: nelems=%d", nelems);
    PostGeoDa *geoda = new PostGeoDa(nelems, fids);

    foreach (l, lwgeoms) {
        LWGEOM *lwgeom = (LWGEOM *) (lfirst(l));
        if (lwgeom_is_empty(lwgeom)) {
            lwdebug(4, "build_pg_geoda: addnullgeometry()");
            geoda->AddNullGeometry();
        } else {
            if (!b_maptype) {
                lwdebug(1, "build_pg_geoda: geom_type=%d", lwgeom->type);
                b_maptype = true;
                geoda->SetMapType(lwgeom->type);
            }
            if (lwgeom->type == POINTTYPE) {
                lwdebug(4, "build_pg_geoda: add point");
                LWPOINT *pt = lwgeom_as_lwpoint(lwgeom);
                geoda->AddPoint(pt);
            } else if (lwgeom->type == MULTIPOINTTYPE) {
                lwdebug(4, "build_pg_geoda: add multi point");
                LWMPOINT *mpt = lwgeom_as_lwmpoint(lwgeom);
                geoda->AddMultiPoint(mpt);
            } else if (lwgeom->type == POLYGONTYPE) {
                lwdebug(4, "build_pg_geoda: add polygon");
                LWPOLY *poly = lwgeom_as_lwpoly(lwgeom);
                geoda->AddPolygon(poly);
            } else if (lwgeom->type == MULTIPOLYGONTYPE) {
                lwdebug(4, "build_pg_geoda: add multi-polygon");
                LWMPOLY *mpoly = lwgeom_as_lwmpoly(lwgeom);
                geoda->AddMultiPolygon(mpoly);
            } else {
                lwdebug(4, "Unknown WKB type %s\n", lwtype_name(lwgeom->type));
                geoda->AddNullGeometry();
            }
        }
        /* Free both the original and geometries */
        lwgeom_free(lwgeom);
    }
    return geoda;
}

double get_min_distthreshold(List *lfids, List *lwgeoms, bool is_arc, bool is_mile)
{
    lwdebug(1,"Enter get_min_distthreshold.");
    PostGeoDa* geoda = build_pg_geoda(lfids, lwgeoms);
    double d = geoda->GetMinDistThreshold(is_arc, is_mile);
    delete geoda;
    lwdebug(1,"Exit get_min_distthreshold.");
    return d;
}

PGWeight* create_cont_weights(List *lfids, List *lwgeoms, bool is_queen, int order, bool inc_lower, double precision_threshold)
{
    lwdebug(1,"Enter create_queen_weights.");
    PostGeoDa* geoda = build_pg_geoda(lfids, lwgeoms);
    PGWeight *w = geoda->CreateContWeights(is_queen, order, inc_lower, precision_threshold);
    delete geoda;
    lwdebug(1,"Exit create_queen_weights.");
    return w;
}

PGWeight* create_knn_weights(List *lfids, List *lwgeoms, int k, double power,
                             bool is_inverse, bool is_arc, bool is_mile)
{
    lwdebug(1,"Enter create_knn_weights.");
    PostGeoDa* geoda = build_pg_geoda(lfids, lwgeoms);
    PGWeight *w = geoda->CreateKnnWeights(k, power, is_inverse, is_arc, is_mile);
    delete geoda;
    lwdebug(1,"Exit create_knn_weights.");
    return w;
}

PGWeight* create_knn_weights_sub(List *lfids, List *lwgeoms, int k, int start, int end, double power,
                             bool is_inverse, bool is_arc, bool is_mile)
{
    lwdebug(1,"Enter create_knn_weights_sub.");
    PostGeoDa* geoda = build_pg_geoda(lfids, lwgeoms);
    PGWeight *w = geoda->CreateKnnWeightsSub(k, start, end, power, is_inverse, is_arc, is_mile);
    delete geoda;
    lwdebug(1,"Exit create_knn_weights_sub.");
    return w;
}

PGWeight* create_kernel_knn_weights(List *lfids, List *lwgeoms, int k, double power,
                                    bool is_inverse, bool is_arc, bool is_mile,
                                    const char* kernel,
                                    double bandwidth, bool adaptive_bandwidth,
                                    bool use_kernel_diagonal)
{
    lwdebug(1,"Enter create_kernel_knn_weights.");
    PostGeoDa* geoda = build_pg_geoda(lfids, lwgeoms);
    PGWeight *w = geoda->CreateKnnWeights(k, power, is_inverse, is_arc, is_mile, kernel, bandwidth,
                                          adaptive_bandwidth, use_kernel_diagonal);
    delete geoda;
    lwdebug(1,"Exit create_kernel_knn_weights.");
    return w;
}

PGWeight* create_distance_weights(List *lfids, List *lwgeoms, double threshold, double power,
                                  bool is_inverse, bool is_arc, bool is_mile)
{
    lwdebug(1,"Enter create_distance_weights.");
    PostGeoDa* geoda = build_pg_geoda(lfids, lwgeoms);
    PGWeight *w = geoda->CreateDistanceWeights(threshold, power, is_inverse, is_arc, is_mile);
    delete geoda;
    lwdebug(1,"Exit create_distance_weights.");
    return w;
}

PGWeight* create_kernel_weights(List *lfids, List *lwgeoms, double bandwidth, double power,
                                bool is_inverse, bool is_arc, bool is_mile, const char* kernel,
                                bool use_kernel_diagonal) {
    lwdebug(1, "Enter create_kernel_weights.");
    PostGeoDa *geoda = build_pg_geoda(lfids, lwgeoms);
    PGWeight *w = geoda->CreateDistanceWeights(bandwidth, power, is_inverse, is_arc, is_mile, kernel,
                                               use_kernel_diagonal);
    delete geoda;
    lwdebug(1, "Exit create_kernel_weights.");
    return w;
}

void free_pglisa(PGLISA *lisa)
{
    if (lisa)    {
        lwfree(lisa->indicators);
        lwfree(lisa->pvalues);
        lwfree(lisa);
    }
}

GalWeight* create_weights(const uint8_t* bw)
{
    const uint8_t *pos = bw; // start position

    // read w type from char
    char w_type;
    memcpy(&w_type, pos, sizeof(char));
    pos += sizeof(char);

    // first int32_t as number of observation
    uint32_t n = 0;
    memcpy(&n, pos, sizeof(uint32_t));
    pos += sizeof(uint32_t);

    GalElement *gl= new GalElement[n];

    for (size_t i=0; i<n; ++i)  {
        uint32_t idx = 0;
        uint16_t n_nbrs = 0;
        uint32_t n_id=0;

        // read idx
        memcpy(&idx, pos, sizeof(uint32_t));
        pos += sizeof(uint32_t);

        gl[i].idx = idx;

        // read number of neighbors
        memcpy(&n_nbrs, pos, sizeof(uint16_t));
        pos += sizeof(uint16_t);

        gl[i].SetSizeNbrs(n_nbrs);

        // read neighbor idx and weights
        std::vector<uint32_t> nbr(n_nbrs);
        std::vector<float> nbrWeight(n_nbrs);

        for (size_t j=0; j<n_nbrs; ++j)  {
            // read neighbor id
            memcpy(&n_id, pos, sizeof(uint32_t));
            pos += sizeof(uint32_t);
            nbr[j] = n_id;
        }

        if (w_type == 'a') {
            for (size_t j=0; j<n_nbrs; ++j) {
                gl[i].SetNbr(j, nbr[j]);
            }
        } else {
            float n_weight;
            for (size_t j=0; j<n_nbrs; ++j)  {
                // read neighbor weight
                memcpy(&n_weight, pos, sizeof(float));
                pos += sizeof(float);
                nbrWeight[j] = n_weight;
            }

            for (size_t j=0; j<n_nbrs; ++j) {
                gl[i].SetNbr(j, nbr[j], nbrWeight[j]);
            }
        }
    }

    GalWeight *w = new GalWeight();
    w->num_obs = n;
    w->is_symmetric = true;
    w->symmetry_checked = true;
    w->gal = gl;
    w->GetNbrStats();

    lwdebug(1, "create_weights(). sparsity=%f", w->GetSparsity());

    return w;
}

PGLISA* wrapper_result(int N, LISA* lisa)
{
    PGLISA *pg_lisa = (PGLISA*)lwalloc(sizeof(PGLISA));
    pg_lisa->n = N;
    pg_lisa->indicators = (double*)lwalloc(sizeof(double) * N);
    pg_lisa->pvalues = (double*)lwalloc(sizeof(double) * N);

    const std::vector<double>& lisa_i = lisa->GetLISAValues();
    const std::vector<double>& lisa_p = lisa->GetLocalSignificanceValues();

    for (size_t i=0; i<N; ++i) {
        pg_lisa->indicators[i] = lisa_i[i];
        pg_lisa->pvalues[i] = lisa_p[i];
    }
    return pg_lisa;
}



double** local_moran_window(int N, const double* r, const uint8_t** bw, const size_t* w_size, int permutations,
                           char *method, double significance_cutoff, int cpu_threads, int seed)
{
    BinWeight* w = new BinWeight(N, bw, w_size);
    int num_obs = w->num_obs; // number of observations in weights in the query Window, == N
    const std::vector<uint32_t>& fids = w->getFids(); // fids starts from 0

    // construct data for observations that may or may NOT be in the Window
    // for those not in the Window, they will be treated as undefined/null
    std::vector<double> data(num_obs, 0);
    std::vector<bool> undefs(num_obs, true);

    for (int i=0; i<N; ++i) {
        data[i] = r[i];
        undefs[i] = false;
    }

    for (int i=0; i<N; ++i) {
        lwdebug(4, "local_joincount_window: data[%d] = %f.", i, data[i]);
    }

    lwdebug(1, "local_moran_window: gda_localmoran().");
    std::string perm_method = "lookup";
    if (method != 0) perm_method = method;

    LISA* lisa = gda_localmoran(w, data, undefs, significance_cutoff, cpu_threads, permutations, perm_method, seed);
    const std::vector<double>& lisa_i = lisa->GetLISAValues();
    const std::vector<double>& lisa_p = lisa->GetLocalSignificanceValues();
    const std::vector<int>& lisa_c = lisa->GetClusterIndicators();

    // results
    double **result = (double **) malloc(sizeof(double*) * N);
    for (int i = 0; i < N; i++) {
        result[i] = (double *) malloc(sizeof(double) * 3);
        result[i][0] = lisa_i[i];
        result[i][1] = lisa_p[i];
        result[i][2] = lisa_c[i];
    }

    // clean
    delete lisa;

    lwdebug(1, "local_moran_window: return results.");
    return result;
}

double ThomasWangHashDouble(uint64_t key) {
    key = (~key) + (key << 21); // key = (key << 21) - key - 1;
    key = key ^ (key >> 24);
    key = (key + (key << 3)) + (key << 8); // key * 265
    key = key ^ (key >> 14);
    key = (key + (key << 2)) + (key << 4); // key * 21
    key = key ^ (key >> 28);
    key = key + (key << 31);
    return 5.42101086242752217E-20 * key;
}

Point* local_moran_fast(double val, const uint8_t* bw, size_t bw_size, int num_obs, const double* vals,
        int permutations, int rnd_seed)
{
    if (num_obs < 1) return NULL;

    // read id and neighbors
    const uint8_t *pos = bw;
    uint32_t idx, n_id=0;
    uint16_t n_nbrs = 0;

    // id
    memcpy(&idx, pos, sizeof(int32_t));
    pos += sizeof(uint32_t);
    idx = idx -1;

    // number of neighbors
    memcpy(&n_nbrs, pos, sizeof(uint16_t));
    pos += sizeof(uint16_t);

    // standardize
    std::vector<double> arr(num_obs, 0);
    for (size_t i=0; i<num_obs; ++i) arr[i] = vals[i];
    GenUtils::StandardizeData(arr);

    lwdebug(6, "pg_local_moran_fast: idx=%d, val=%f, std_val=%f.", idx, val, arr[40]);

    // reassign input val
    val = arr[idx];

    double lisa_i=0, lisa_p=0;
    if (n_nbrs > 0) {
        std::vector<uint32_t> nbr(n_nbrs);
        std::vector<double> nbrWeight(n_nbrs, 1.0);

        for (size_t j = 0; j < n_nbrs; ++j) {
            // read neighbor id
            memcpy(&n_id, pos, sizeof(uint32_t));
            pos += sizeof(uint32_t);
            nbr[j] = n_id - 1;
            lwdebug(6, "pg_local_moran_fast: %d-th neighbor: %d, %f.", j, nbr[j], vals[nbr[j]]);
        }
        if (bw_size > (size_t)(pos - bw)) {
            double n_weight = 1.0;
            for (size_t j = 0; j < n_nbrs; ++j) {
                // read neighbor weight
                memcpy(&n_weight, pos, sizeof(float));
                pos += sizeof(float);
                nbrWeight[j] = n_weight;
            }
        }

        // compute lisa
        double sp_lag = 0;
        for (size_t i=0; i<n_nbrs; ++i) {
            sp_lag += arr[nbr[i]];
            lwdebug(6, "pg_local_moran_fast: %d-th neighbor: %f.", nbr[i], arr[nbr[i]]);
        }
        sp_lag /= n_nbrs;
        lisa_i = val * sp_lag;

        // p-val
        int seed_start = rnd_seed + idx;
        int max_rand = num_obs-1;
        GeoDaSet workPermutation(num_obs);
        std::vector<double> permutedSA(permutations, 0);
        for (size_t perm=0; perm<permutations; perm++) {
            int rand=0, newRandom;
            double rng_val;
            while (rand < n_nbrs) {
                rng_val = ThomasWangHashDouble(seed_start++) * max_rand;
                newRandom = (int)(rng_val<0.0?ceil(rng_val - 0.5):floor(rng_val + 0.5));
                if (newRandom != idx && !workPermutation.Belongs(newRandom) ) {
                    workPermutation.Push(newRandom);
                    rand++;
                }
            }
            std::vector<int> permNeighbors(n_nbrs);
            for (int cp=0; cp<n_nbrs; cp++) {
                permNeighbors[cp] = workPermutation.Pop();
            }
            double permutedLag = 0;
            for (int cp=0; cp<n_nbrs; cp++) {
                int nb = permNeighbors[cp];
                permutedLag += arr[nb];
            }
            permutedLag /= n_nbrs;
            permutedSA[perm] = permutedLag * val;
        }
        uint64_t countLarger = 0;
        for (size_t i=0; i<permutations; ++i) {
            if (permutedSA[i] >= lisa_i) {
                countLarger += 1;
            }
        }
        // pick the smallest counts
        if (permutations-countLarger <= countLarger) {
            countLarger = permutations-countLarger;
        }
        lisa_p = (countLarger+1.0)/(permutations+1);
    }

    lwdebug(1, "local_moran_fast: complete");

    Point *r = (Point *) palloc(sizeof(Point));

    r->x = lisa_i;
    r->y = lisa_p;
    return r;
}

double** local_moran_window_bytea(int N, const int64* fids, const double* r, const uint8_t* bw)
{
    BinWeight* w = new BinWeight(bw); // complete weights
    int64 num_obs  = w->num_obs;

    // NOTE: num_obs could be larger than N
    // input values could be a subset of total values
    std::vector<double> data(num_obs +1 , 0);
    std::vector<bool> undefs(num_obs +1, true);

    for (size_t i=0; i<N; ++i) {
        size_t fid = fids[i];
        if (fid > num_obs) {
            lwerror("pg_local_moran: fid not match in weights. fid > w.num_obs %d", fids[i]) ;
        }
        data[fid] = r[i];
        undefs[fid] = false;
        //lwdebug(1, "pg_local_moran: data[%d] = %f, nn=%d", fid, data[fid], w->GetNbrSize(fid));
    }

    lwdebug(1, "pg_local_moran: gda_localmoran()");
    double significance_cutoff = 0.05;
    int nCPUs = 8, permutations = 999, last_seed_used = 123456789;
    std::string perm_method = "complete";
    LISA* lisa = gda_localmoran(w, data, undefs, significance_cutoff, nCPUs, permutations, perm_method, last_seed_used);
    const std::vector<double>& lisa_i = lisa->GetLISAValues();
    const std::vector<double>& lisa_p = lisa->GetLocalSignificanceValues();

    // results
    double **result = (double **) palloc(sizeof(double*) * N);
    for (size_t i = 0; i < N; i++) {
        result[i] = (double*) palloc(sizeof(double) * 2);
        result[i][0] = lisa_i[ fids[i] ];
        result[i][1] = lisa_p[ fids[i] ];
    }

    // clean
    delete lisa;
    lwdebug(1, "Exit pg_local_moran.");
    return result;
}

double** neighbor_match_test_window(List *lfids, List *lwgeoms, int k, int n_vars, int N, const double** r,
                                    double power, bool is_inverse, bool is_arc, bool is_mile,
                                    const char *scale_method, const char* dist_type)
{
    lwdebug(1, "Enter neighbor_match_test_window.");

    // create KNN spatial weights
    PostGeoDa* geoda = build_pg_geoda(lfids, lwgeoms);

    std::string poly_id = "", kernel = "";
    double bandwidth = 0;
    bool adaptive_bandwidth = false, use_kernel_diagonal = false;
    GeoDaWeight* w = gda_knn_weights(geoda, k, power, is_inverse, is_arc, is_mile, kernel, bandwidth,
                                     adaptive_bandwidth, use_kernel_diagonal, poly_id);

    int num_obs = w->num_obs;

    lwdebug(1, "Enter CreateKnnWeights: num_obs=%d", w->num_obs);
    lwdebug(1, "Enter CreateKnnWeights: min_nbrs=%d", w->GetMinNbrs());
    lwdebug(1, "Enter CreateKnnWeights: sparsity=%f", w->GetSparsity());

    std::vector<std::vector<double> > data_arr;
    std::vector<std::vector<bool> > undefs_arr;

    for (int i=0; i< n_vars; ++i) {
        std::vector<double> data(num_obs, 0);
        std::vector<bool> undefs(num_obs, true);
        data_arr.push_back(data);
        undefs_arr.push_back(undefs);
    }

    for (int i=0; i<N; ++i) {
        for (int j=0; j< n_vars; ++j) {
            data_arr[j][i] = r[i][j];
            undefs_arr[j][i] = false;
        }
        //lwdebug(1, "r[%d][0,1]=%f,%f", i, r[i][0], r[i][1]);
    }

    lwdebug(1, "neighbor_match_test_window: neighbor_match_test().");

    std::string scale_method_str = "standardize";
    if (scale_method != 0) scale_method_str = scale_method;

    std::string dist_type_str = "euclidean";
    if (dist_type != 0) dist_type_str = dist_type;

    std::vector<std::vector<double> > nmt = gda_neighbor_match_test(w, k, data_arr, scale_method_str, dist_type_str);

    // results
    double **result = (double **) palloc(sizeof(double*) * N);
    for (int i = 0; i < N; i++) {
        result[i] = (double *) palloc(sizeof(double) * 2);
        result[i][0] = nmt[0][i];
        result[i][1] = nmt[1][i];
    }

    // clean
    delete w;
    delete geoda;

    lwdebug(1, "neighbor_match_test_window: return results.");
    return result;
}