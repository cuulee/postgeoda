/**
 * Author: Xun Li <lixun910@gmail.com>
 *
 * Changes:
 * 2021-1-27 Update to use libgeoda 0.0.6;
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

PostGeoDa* build_pg_geoda(List *lfids, List *lwgeoms) {
    lwdebug(1, "Enter build_pg_geoda.");

    ListCell *l;
    size_t nelems = list_length(lwgeoms);
    bool b_maptype = false;
    size_t count = 0;

    std::vector<uint32_t> fids(nelems, 0);
    foreach(l, lfids) {
        fids[count] = lfirst_int(l);
        lwdebug(5, "build_pg_geoad fids=%d", fids[count]);
        count++;
    }

    lwdebug(1, "build_pg_geoad: nelems=%d", nelems);
    PostGeoDa *geoda = new PostGeoDa(nelems, fids);

    foreach (l, lwgeoms) {
        LWGEOM *lwgeom = (LWGEOM *) (lfirst(l));
        if (lwgeom_is_empty(lwgeom)) {
            lwdebug(4, "build_pg_geoad: addnullgeometry()");
            geoda->AddNullGeometry();
        } else {
            if (!b_maptype) {
                lwdebug(1, "build_pg_geoad: geom_type=%d", lwgeom->type);
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

PGWeight* create_queen_weights(List *lfids, List *lwgeoms, int order, bool inc_lower, double precision_threshold)
{
    lwdebug(1,"Enter create_queen_weights.");
    PostGeoDa* geoda = build_pg_geoda(lfids, lwgeoms);
    PGWeight *w = geoda->CreateQueenWeights(order, inc_lower, precision_threshold);
    delete geoda;
    lwdebug(1,"Exit create_queen_weights.");
    return w;
}

PGWeight* create_knn_weights(List *lfids, List *lwgeoms, int k)
{
    lwdebug(1,"Enter create_knn_weights.");
    PostGeoDa* geoda = build_pg_geoda(lfids, lwgeoms);
    PGWeight *w = geoda->CreateKnnWeights(k);
    delete geoda;
    lwdebug(1,"Exit create_knn_weights.");
    return w;
}

PGWeight* create_distance_weights(List *lfids, List *lwgeoms, double threshold)
{
    lwdebug(1,"Enter create_distance_weights.");
    PostGeoDa* geoda = build_pg_geoda(lfids, lwgeoms);
    PGWeight *w = geoda->CreateKnnWeights(4);
    delete geoda;
    lwdebug(1,"Exit create_distance_weights.");
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

Point** pg_local_moran(int N, const int64* fids, const double* r, const uint8_t* bw)
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
    Point **result = (Point **) palloc(sizeof(Point*) * N);
    for (size_t i = 0; i < N; i++) {
        result[i] = (Point *) palloc(sizeof(Point));
        result[i]->x = lisa_i[ fids[i] ];
        result[i]->y = lisa_p[ fids[i] ];
    }

    // clean
    delete lisa;
    lwdebug(1, "Exit pg_local_moran.");
    return result;
}


Point** pg_local_moran_warray(int N, const double* r, const uint8_t** bw, const size_t* w_size)
{
    BinWeight* w = new BinWeight(N, bw, w_size);
    int num_obs = w->num_obs;
    const std::vector<uint32_t>& fids = w->getFids();

    // check if w matches input fids

    std::vector<double> data(num_obs+1, 0); // fid starts from 0, which will be ignored
    std::vector<bool> undefs(num_obs+1, true);

    for (size_t i=0; i<N; ++i) {
        size_t fid = fids[i];
        if (fid > num_obs)
            lwerror("pg_local_moran_warray: %d-th fid = %d.", i, fid);
        data[fid] = r[i];
        undefs[fid] = false;
    }

    for (size_t i=0; i<N; ++i) {
        lwdebug(2, "pg_local_moran_warray: data[%d] = %f.", i, data[i]);
    }

    lwdebug(1, "pg_local_moran_warray: gda_localmoran().");
    double significance_cutoff = 0.05;
    int nCPUs = 8, permutations = 999, last_seed_used = 123456789;
    std::string perm_method = "complete";
    LISA* lisa = gda_localmoran(w, data, undefs, significance_cutoff, nCPUs, permutations, perm_method, last_seed_used);
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

    lwdebug(1, "pg_local_moran_warray: return results.");
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

Point* pg_local_moran_fast(double val, const uint8_t* bw, size_t bw_size, int num_obs, const double* vals,
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

    lwdebug(1, "pg_local_moran_fast: complete");

    Point *r = (Point *) palloc(sizeof(Point));

    r->x = lisa_i;
    r->y = lisa_p;
    return r;
}