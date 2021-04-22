/**
 * Author: Xun Li <lixun910@gmail.com>
 *
 * Changes:
 * 2021-4-9 add pg_quantilelisa()
 */

#include <vector>

#include <libgeoda/gda_sa.h>
#include <libgeoda/sa/LISA.h>
#include <libgeoda/GeoDaSet.h>
#include <libgeoda/pg/geoms.h>
#include <libgeoda/pg/utils.h>

#include "binweight.h"
#include "proxy.h"

/**
 * Window function of Quantile LISA
 *
 * @param k
 * @param quantile
 * @param N
 * @param r
 * @param bw
 * @param w_size
 * @return
 */
Point** pg_quantilelisa(int k, int quantile, int N, const double* r, const uint8_t** bw, const size_t* w_size)
{
    BinWeight* w = new BinWeight(N, bw, w_size);
    int num_obs = w->num_obs;
    const std::vector<uint32_t>& fids = w->getFids();

    // check if w matches input fids
    std::vector<double> data(num_obs+1, 0); // fid starts from 0, which will be ignored
    std::vector<bool> undefs(num_obs+1, true);

    for (int i=0; i<N; ++i) {
        int fid = fids[i];
        if (fid > num_obs) {
            lwerror("pg_quantilelisa: %d-th fid = %d.", i, fid);
        }
        data[fid] = r[i];
        undefs[fid] = false;
    }

    for (int i=0; i<N; ++i) {
        lwdebug(2, "pg_quantilelisa: data[%d] = %f.", i, data[i]);
    }

    lwdebug(1, "pg_quantilelisa: gda_quantilelisa().");

    double significance_cutoff = 0.05;
    int nCPUs = 8, permutations = 999, last_seed_used = 123456789;
    std::string perm_method = "complete";
    LISA* lisa = gda_quantilelisa(w, k, quantile, data, undefs, significance_cutoff, nCPUs, permutations, perm_method, last_seed_used);
    const std::vector<double>& lisa_i = lisa->GetLISAValues();
    const std::vector<double>& lisa_p = lisa->GetLocalSignificanceValues();

    // results
    Point **result = (Point **) palloc(sizeof(Point*) * N);
    for (size_t i = 0; i < N; i++) {
        result[i] = (Point *) palloc(sizeof(Point));
        result[i]->x = lisa_i[ fids[i] ];
        result[i]->y = lisa_p[ fids[i] ];
    }

    // clean
    delete lisa;

    lwdebug(1, "pg_quantilelisa: return results.");
    return result;
}