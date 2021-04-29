/**
 * Author: Xun Li <lixun910@gmail.com>
 *
 * Changes:
 * 2021-4-9 add pg_quantilelisa()
 * 2021-4-28 Update functions with new BinWeight() constructor for Window query; add local_multiquantilelisa_window()
 */

#include <vector>

#include <libgeoda/gda_sa.h>
#include <libgeoda/sa/LISA.h>
#include <libgeoda/GeoDaSet.h>
#include <libgeoda/pg/geoms.h>
#include <libgeoda/pg/utils.h>

#include "binweight.h"
#include "proxy.h"

double** local_quantilelisa_window(int k, int quantile, int N, const double* r, const uint8_t** bw,
                                   const size_t* w_size, int permutations, char *method, double significance_cutoff,
                                   int cpu_threads, int seed)
{
    BinWeight* w = new BinWeight(N, bw, w_size);
    int num_obs = w->num_obs;

    std::vector<double> data(num_obs, 0);
    std::vector<bool> undefs(num_obs, true);

    for (int i=0; i<N; ++i) {
        data[i] = r[i];
        undefs[i] = false;
    }

    lwdebug(1, "local_quantilelisa_window: gda_quantilelisa().");
    std::string perm_method = "lookup";
    if (method != 0) perm_method = method;

    LISA* lisa = gda_quantilelisa(w, k, quantile, data, undefs, significance_cutoff, cpu_threads, permutations,
                                  perm_method, seed);
    const std::vector<double>& lisa_i = lisa->GetLISAValues();
    const std::vector<double>& lisa_p = lisa->GetLocalSignificanceValues();
    const std::vector<int>& lisa_c = lisa->GetNumNeighbors();

    // results
    double **result = (double **) palloc(sizeof(double*) * N);
    for (size_t i = 0; i < N; i++) {
        result[i] = (double *) palloc(sizeof(double) * 3);
        result[i][0] = lisa_i[i];
        result[i][1] = lisa_p[i];
        result[i][2] = lisa_c[i];
    }

    // clean
    delete lisa;

    lwdebug(1, "local_quantilelisa_window: return results.");
    return result;
}

double** local_multiquantilelisa_window(int n_vars, int* k, int* quantile, int N, const double** r, const uint8_t** bw,
                                        const size_t* w_size, int permutations, char *method,
                                        double significance_cutoff, int cpu_threads, int seed)
{
    lwdebug(1, "Enter local_multiquantilelisa_window.");

    BinWeight* w = new BinWeight(N, bw, w_size);
    int num_obs = w->num_obs;

    std::vector<std::vector<double> > data_arr;
    std::vector<std::vector<bool> > undefs_arr;
    std::vector<int> k_arr, quantile_arr;

    for (int i=0; i< n_vars; ++i) {
        std::vector<double> data(num_obs, 0);
        std::vector<bool> undefs(num_obs, true);
        data_arr.push_back(data);
        undefs_arr.push_back(undefs);
        k_arr.push_back(k[i]);
        quantile_arr.push_back(quantile[i]);
    }

    for (int i=0; i<N; ++i) {
        for (int j=0; j< n_vars; ++j) {
            data_arr[j][i] = r[i][j];
            undefs_arr[j][i] = false;
        }
    }

    lwdebug(1, "local_multiquantilelisa_window: gda_quantilelisa().");

    std::string perm_method = "lookup";
    if (method != 0) perm_method = method;

    LISA* lisa = gda_multiquantilelisa(w, k_arr, quantile_arr, data_arr, undefs_arr, significance_cutoff, cpu_threads,
                                       permutations, perm_method, seed);
    const std::vector<double>& lisa_i = lisa->GetLISAValues();
    const std::vector<double>& lisa_p = lisa->GetLocalSignificanceValues();
    const std::vector<int>& lisa_c = lisa->GetNumNeighbors();

    // results
    double **result = (double **) palloc(sizeof(double*) * N);
    for (size_t i = 0; i < N; i++) {
        result[i] = (double *) palloc(sizeof(double) * 3);
        result[i][0] = lisa_i[i];
        result[i][1] = lisa_p[i];
        result[i][2] = lisa_c[i];
    }

    // clean
    delete lisa;

    lwdebug(1, "local_multiquantilelisa_window: return results.");
    return result;
}