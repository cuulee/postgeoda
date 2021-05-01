/**
 * Author: Xun Li <lixun910@gmail.com>
 *
 * Changes:
 * 2021-4-30 add redcap_window()
 */

#include <vector>

#include <libgeoda/gda_sa.h>
#include <libgeoda/sa/LISA.h>
#include <libgeoda/weights/GalWeight.h>
#include <libgeoda/GeoDaSet.h>
#include <libgeoda/GenUtils.h>
#include <libgeoda/pg/geoms.h>
#include <libgeoda/pg/utils.h>
#include <libgeoda/gda_clustering.h>

#include "binweight.h"
#include "postgeoda.h"
#include "proxy.h"

int* skater1_window(int k, int N, int n_vars, const double** r, const uint8_t** bw, const size_t* w_size,
                    int min_region, const char *scale_type, const char* dist_type, int seed, int cpu_threads)
{
    lwdebug(1, "Enter skater_window.");

    BinWeight* w = new BinWeight(N, bw, w_size);
    int num_obs = w->num_obs;

    if (w->CheckConnectivity() == false) {
        lwdebug(1, "skater_window: check connectivity failed.");
        delete w;
        return 0;
    }
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
    }

    lwdebug(1, "skater_window: call gda_skater(k=%d)", k);

    std::vector<double> bound_vals;
    if (min_region > 0) {
        for (int i = 0; i < N; ++i) {
            bound_vals.push_back(1.0);
        }
    }
    double min_bound = min_region;

    std::string scale_method = "standardize";
    std::string distance_method = "euclidean";

    if (scale_type!= 0) scale_method= scale_type;
    if (dist_type!= 0) distance_method= dist_type;

    std::vector<std::vector<int> > cluster_ids = gda_skater(k, w, data_arr, scale_method, distance_method, bound_vals,
                                                         min_bound, seed, cpu_threads);

    std::vector<int> clusters = GenUtils::flat_2dclusters(N, cluster_ids);

    // results
    int *result = (int*) palloc(sizeof(int*) * N);
    for (int i = 0; i < N; i++) {
        result[i] = clusters[i];
    }

    // clean
    delete w;

    lwdebug(1, "skater_window: return results.");
    return result;
}

int* skater2_window(int k, int N, int n_vars, const double** r, const uint8_t** bw, const size_t* w_size,
                    const double* bound_var, double min_bound, const char *scale_type, const char* dist_type,
                    int seed, int cpu_threads)
{
    lwdebug(1, "Enter skater2_window.");

    BinWeight* w = new BinWeight(N, bw, w_size);
    int num_obs = w->num_obs;

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
    }

    lwdebug(1, "skater2_window: call gda_skater");

    std::vector<double> bound_vals;
    if (bound_var) {
        for (int i = 0; i < N; ++i) {
            bound_vals.push_back(bound_var[i]);
        }
    }

    std::string scale_method = "standardize";
    std::string distance_method = "euclidean";

    if (scale_type!= 0) scale_method= scale_type;
    if (dist_type!= 0) distance_method= dist_type;

    std::vector<std::vector<int> > cluster_ids = gda_skater(k, w, data_arr, scale_method, distance_method, bound_vals,
                                                            min_bound, seed, cpu_threads);

    std::vector<int> clusters = GenUtils::flat_2dclusters(N, cluster_ids);

    // results
    int *result = (int*) palloc(sizeof(int*) * N);
    for (int i = 0; i < N; i++) {
        result[i] = clusters[i];
    }

    // clean
    delete w;

    lwdebug(1, "skater2_window: return results.");
    return result;
}