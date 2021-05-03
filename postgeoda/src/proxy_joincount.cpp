/**
 * Author: Xun Li <lixun910@gmail.com>
 *
 * Changes:
 * 2021-1-27 Update to use libgeoda 0.0.6; add pg_local_joincount()
 * 2021-4-27 Change to local_joincount_window(), local_bijoincount_window(),
 * local_multijoincount_window()
 * 2021-4-28 Update functions with new BinWeight() constructor for Window query
 */

#include <vector>

#include <libgeoda/gda_sa.h>
#include <libgeoda/sa/LISA.h>
#include <libgeoda/weights/GalWeight.h>
#include <libgeoda/GeoDaSet.h>
#include <libgeoda/GenUtils.h>
#include <libgeoda/pg/geoms.h>
#include <libgeoda/pg/utils.h>

#include "binweight.h"
#include "postgeoda.h"
#include "proxy.h"

double** local_joincount_window(int N, const double* r, const uint8_t** bw, const size_t* w_size, int permutations,
                                char *method, double significance_cutoff, int cpu_threads, int seed)
{
    BinWeight* w = new BinWeight(N, bw, w_size);
    int num_obs = w->num_obs;

    std::vector<double> data(num_obs, 0);
    std::vector<bool> undefs(num_obs, true);

    for (int i=0; i<N; ++i) {
        data[i] = r[i];
        undefs[i] = false;
    }

    std::string perm_method = "lookup";
    if (method != 0) perm_method = method;

    LISA* lisa = gda_localjoincount(w, data, undefs, significance_cutoff, cpu_threads, permutations, perm_method, seed);
    const std::vector<double>& lisa_i = lisa->GetLISAValues();
    const std::vector<double>& lisa_p = lisa->GetLocalSignificanceValues();
    const std::vector<int>& lisa_c = lisa->GetNumNeighbors();

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

    lwdebug(1, "local_joincount_window: return results.");
    return result;
}

double** local_bijoincount_window(int N, const double* r1, const double* r2, const uint8_t** bw, const size_t* w_size, int permutations,
                                  char *method, double significance_cutoff, int cpu_threads, int seed)
{
    BinWeight* w = new BinWeight(N, bw, w_size);
    int num_obs = w->num_obs;

    // check if w matches input fids

    std::vector<double> data1(num_obs, 0);
    std::vector<bool> undefs1(num_obs, true);
    std::vector<double> data2(num_obs, 0);
    std::vector<bool> undefs2(num_obs, true);

    for (int i=0; i<N; ++i) {
        data1[i] = r1[i];
        data2[i] = r2[i];
        undefs1[i] = false;
        undefs2[i] = false;
    }

    std::vector<std::vector<double> > data;
    data.push_back(data1);
    data.push_back(data2);

    std::vector<std::vector<bool> > undefs;
    undefs.push_back(undefs1);
    undefs.push_back(undefs2);

    std::string perm_method = "lookup";
    if (method != 0) perm_method = method;

    LISA* lisa = gda_localmultijoincount(w, data, undefs, significance_cutoff, cpu_threads, permutations, perm_method, seed);
    const std::vector<double>& lisa_i = lisa->GetLISAValues();
    const std::vector<double>& lisa_p = lisa->GetLocalSignificanceValues();
    const std::vector<int>& lisa_c = lisa->GetNumNeighbors();

    // results
    double **result = (double **) malloc(sizeof(double*) * N);
    for (size_t i = 0; i < N; i++) {
        result[i] = (double *) malloc(sizeof(double) * 3);
        result[i][0] = lisa_i[i];
        result[i][1] = lisa_p[i];
        result[i][2] = lisa_c[i];
    }

    // clean
    delete lisa;

    lwdebug(1, "local_bijoincount_window: return results.");
    return result;
}

double** local_multijoincount_window(int N, int n_vars, const double** r, const uint8_t** bw, const size_t* w_size, int permutations,
                                    char *method, double significance_cutoff, int cpu_threads, int seed)
{
    lwdebug(1, "Enter local_multijoincount_window.");

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

    lwdebug(1, "local_multijoincount_window: call gda_localmultijoincount");

    std::string perm_method = "lookup";
    if (method != 0) perm_method = method;

    LISA* lisa = gda_localmultijoincount(w, data_arr, undefs_arr, significance_cutoff, cpu_threads, permutations,
                                         perm_method, seed);
    const std::vector<double>& lisa_i = lisa->GetLISAValues();
    const std::vector<double>& lisa_p = lisa->GetLocalSignificanceValues();
    const std::vector<int>& lisa_c = lisa->GetNumNeighbors();

    // results
    double **result = (double **) malloc(sizeof(double*) * N);
    for (size_t i = 0; i < N; i++) {
        result[i] = (double *) malloc(sizeof(double) * 3);
        result[i][0] = lisa_i[i];
        result[i][1] = lisa_p[i];
        result[i][2] = lisa_c[i];
    }

    // clean
    delete lisa;

    lwdebug(1, "local_multijoincount_window: return results.");
    return result;
}
