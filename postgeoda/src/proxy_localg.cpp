/**
 * Author: Xun Li <lixun910@gmail.com>
 *
 * Changes:
 * 2021-1-29 Update to use libgeoda 0.0.6; add pg_local_g();
 * add pg_local_gstar()
 * 2021-4-28 Update functions with new BinWeight() constructor for Window query
 */

#include <vector>

#include <libgeoda/gda_sa.h>
#include <libgeoda/sa/LISA.h>
#include <libgeoda/GeoDaSet.h>
#include <libgeoda/pg/geoms.h>
#include <libgeoda/pg/utils.h>

#include "binweight.h"
#include "proxy.h"

double** local_g_window(int N, const double* r, const uint8_t** bw, const size_t* w_size, int permutations,
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

    lwdebug(1, "local_g_window: gda_localg().");
    std::string perm_method = "lookup";
    if (method != 0) perm_method = method;

    LISA* lisa = gda_localg(w, data, undefs, significance_cutoff, cpu_threads, permutations, perm_method, seed);
    const std::vector<double>& lisa_i = lisa->GetLISAValues();
    const std::vector<double>& lisa_p = lisa->GetLocalSignificanceValues();
    const std::vector<int>& lisa_c = lisa->GetClusterIndicators();

    // results
    double **result = (double **) palloc(sizeof(double*) * N);
    for (int i = 0; i < N; i++) {
        result[i] = (double *) palloc(sizeof(double) * 3);
        result[i][0] = lisa_i[i];
        result[i][1] = lisa_p[i];
        result[i][2] = lisa_c[i];
    }

    // clean
    delete lisa;

    lwdebug(1, "local_g_window: return results.");
    return result;
}

double** local_gstar_window(int N, const double* r, const uint8_t** bw, const size_t* w_size, int permutations,
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

    lwdebug(1, "local_gstar_window: gda_localg().");
    std::string perm_method = "lookup";
    if (method != 0) perm_method = method;

    LISA* lisa = gda_localgstar(w, data, undefs, significance_cutoff, cpu_threads, permutations, perm_method, seed);
    const std::vector<double>& lisa_i = lisa->GetLISAValues();
    const std::vector<double>& lisa_p = lisa->GetLocalSignificanceValues();
    const std::vector<int>& lisa_c = lisa->GetClusterIndicators();

    // results
    double **result = (double **) palloc(sizeof(double*) * N);
    for (int i = 0; i < N; i++) {
        result[i] = (double *) palloc(sizeof(double) * 3);
        result[i][0] = lisa_i[i];
        result[i][1] = lisa_p[i];
        result[i][2] = lisa_c[i];
    }

    // clean
    delete lisa;

    lwdebug(1, "local_gstar_window: return results.");
    return result;
}
