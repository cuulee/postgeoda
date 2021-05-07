/**
 * Author: Xun Li <lixun910@gmail.com>
 *
 * Changes:
 * 2021-5-6 add local_geary_window(); local_multigeary_window()
 */

#include <vector>

#include <libgeoda/gda_sa.h>
#include <libgeoda/sa/LISA.h>
#include <libgeoda/GeoDaSet.h>
#include <libgeoda/pg/geoms.h>
#include <libgeoda/pg/utils.h>

#include "binweight.h"
#include "proxy.h"

double** local_geary_window(int N, const double* r, const uint8_t** bw, const size_t* w_size, int permutations,
                        char *method, double significance_cutoff, int cpu_threads, int seed)
{
    BinWeight* w = new BinWeight(N, bw, w_size); // weights in Window
    int num_obs = w->num_obs;

    std::vector<double> data(num_obs, 0);
    std::vector<bool> undefs(num_obs, true);

    for (int i=0; i<N; ++i) {
        data[i] = r[i];
        undefs[i] = false;
    }

    lwdebug(1, "local_geary_window: gda_localgeary().");
    std::string perm_method = "lookup";
    if (method != 0) perm_method = method;

    LISA* lisa = gda_localgeary(w, data, undefs, significance_cutoff, cpu_threads, permutations, perm_method, seed);
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

    lwdebug(1, "local_geary_window: return results.");
    return result;
}

double** local_multigeary_window(int n_vars, int N, const double** r, const uint8_t** bw,
                                 const size_t* w_size, int permutations, char *method,
                                 double significance_cutoff, int cpu_threads, int seed)
{
    lwdebug(1, "Enter local_multigeary_window.");

    BinWeight* w = new BinWeight(N, bw, w_size); // weights in Window
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

    lwdebug(1, "local_multigeary_window: gda_local_multigeary().");
    std::string perm_method = "lookup";
    if (method != 0) perm_method = method;

    LISA* lisa = gda_localmultigeary(w, data_arr, undefs_arr, significance_cutoff, cpu_threads, permutations,
            perm_method, seed);

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

    lwdebug(1, "Exit local_multigeary_window: return results.");
    return result;
}
