/**
 * Author: Xun Li <lixun910@gmail.com>
 *
 * Changes:
 * 2021-1-27 Update to use libgeoda 0.0.6
 * 2021-4-28 Update constructor: when creating weights from bytea array in a query Window, remove the neighbors not in
 * the query window
 */

#include <libgeoda/pg/utils.h>
#include "binweight.h"


BinElement::BinElement() {

}

BinElement::~BinElement() {

}

uint32_t BinElement::getIdx() const {
    return idx;
}

void BinElement::setIdx(uint32_t idx) {
    BinElement::idx = idx;
}

void BinElement::setNbrIds(const std::vector<long> &nbrIds) {
    nbr_ids = nbrIds;
}

void BinElement::setNbrWeights(const std::vector<double> &nbrWeights) {
    nbr_weights = nbrWeights;
}

bool BinElement::checkNeighbor(uint32_t nbrIdx) {
    // brutal force here , should be replaced by dict() check
    for (size_t i=0; i<nbr_ids.size(); ++i) {
        if (nbr_ids[i] == nbrIdx) {
            return true;
        }
    }
    return false;
}

const std::vector<long> &BinElement::getNbrIds() const {
    return nbr_ids;
}

const std::vector<double> &BinElement::getNbrWeights() const {
    return nbr_weights;
}

size_t BinElement::getSize() {
    return nbr_ids.size();
}

/**
 * Create a full binary weights object
 *
 * @param bw
 */
BinWeight::BinWeight(const uint8_t* bw)
{
    const uint8_t *pos = bw; // start position

    // read w type from char
    char w_type;
    memcpy(&w_type, pos, sizeof(char));
    pos += sizeof(char);

    if (w_type == 'a') weight_type = gal_type;
    else weight_type = gwt_type;

    // first int32_t as number of observation
    uint32_t n = 0;
    memcpy(&n, pos, sizeof(uint32_t));
    pos += sizeof(uint32_t);

    this->num_obs = n;

    for (size_t i=0; i<n; ++i)  {
        uint32_t idx = 0;
        uint16_t n_nbrs = 0;
        uint32_t n_id=0;
        BinElement *gl = new BinElement;

        // read idx
        memcpy(&idx, pos, sizeof(uint32_t));
        pos += sizeof(uint32_t);

        gl->setIdx(idx);

        // read number of neighbors
        memcpy(&n_nbrs, pos, sizeof(uint16_t));
        pos += sizeof(uint16_t);

        // read neighbor idx and weights
        std::vector<long> nbr(n_nbrs);

        for (size_t j=0; j<n_nbrs; ++j)  {
            // read neighbor id
            memcpy(&n_id, pos, sizeof(uint32_t));
            pos += sizeof(uint32_t);
            nbr[j] = n_id;
        }

        gl->setNbrIds(nbr);

        if (w_type == 'w') {
            std::vector<double> nbrWeight(n_nbrs);
            float n_weight;

            for (size_t j=0; j<n_nbrs; ++j)  {
                // read neighbor weight
                memcpy(&n_weight, pos, sizeof(float));
                pos += sizeof(float);
                nbrWeight[j] = n_weight;
            }

            gl->setNbrWeights(nbrWeight);
        }

        this->w_dict[idx] = gl;
    }
    //this->GetNbrStats();

    mask.resize(this->num_obs, true);
}

/**
 * Create weights from bytea in Window
 *
 * The weights will be constructed by removing the neighbors that are not in the query window
 *
 * @param N the length of the rows of weights (bytea)
 * @param bw the content (byte) of all weights
 * @param w_size the size (byte) of weights in each row
 */
BinWeight::BinWeight(int N, const uint8_t** bw, const size_t* w_size)
{
    boost::unordered_map<uint32_t, size_t> fid_dict;

    // get fids from the Window
    for (size_t i=0; i<N; ++i)  {
        const uint8_t *pos = bw[i];
        uint32_t fid;
        // idx
        memcpy(&fid, pos, sizeof(uint32_t));
        pos += sizeof(uint32_t);
        // mapping fid to index
        fid_dict[fid] = i;
    }

    // update the weights by removing neighbors that are not in the query Window
    for (size_t i=0; i<N; ++i)  {
        BinElement *gl= new BinElement;
        const uint8_t *pos = bw[i];

        // idx
        uint32_t fid;
        memcpy(&fid, pos, sizeof(uint32_t));
        pos += sizeof(uint32_t);

        // number of neighbors
        uint16_t n_nbrs;
        memcpy(&n_nbrs, pos, sizeof(uint16_t));
        pos += sizeof(uint16_t);

        // read neighbor id
        std::vector<long> nbr;
        std::vector<bool> nbr_flag(n_nbrs, false);
        uint32_t n_id;

        for (size_t j=0; j<n_nbrs; ++j)  {
            // read neighbor id
            memcpy(&n_id, pos, sizeof(uint32_t));
            pos += sizeof(uint32_t);
            if (fid_dict.find(n_id) != fid_dict.end()) {
                nbr.push_back( fid_dict[n_id] );
                nbr_flag[j] = true;
            }
        }
        gl->setNbrIds(nbr);

        // read neighbor weights, if needed
        if (w_size[i] > (size_t)(pos - bw[i])) {
            std::vector<double> nbrWeight;
            float n_weight = 0;
            for (size_t j = 0; j < n_nbrs; ++j) {
                // read neighbor weight
                memcpy(&n_weight, pos, sizeof(float));
                pos += sizeof(float);
                if (nbr_flag[j] && fid_dict.find(n_id) != fid_dict.end()) {
                    nbrWeight.push_back(n_weight);
                }
            }
            gl->setNbrWeights(nbrWeight);
        }

        this->w_dict[ fid_dict[fid] ] = gl;
        this->fids.push_back(fid);
    }

    this->num_obs = N;
    //this->GetNbrStats();

    mask.resize(this->num_obs, true);

    lwdebug(1, "create_weights_from_barray(). sparsity=%f", this->GetSparsity());
}

/**
 * Create binary weights for lisa fast (partition) algorithms.
 *
 * @param N  the number of rows in the query window
 * @param NN  the number of observations (NN > N)
 * @param bw
 * @param w_size
 */
BinWeight::BinWeight(int N, int NN, const uint8_t** bw, const size_t* w_size)
{
    mask.resize(NN, false); // mask all observations first

    for (size_t i=0; i<N; ++i)  {
        BinElement *gl= new BinElement;
        const uint8_t *pos = bw[i];

        // idx
        uint32_t fid;
        memcpy(&fid, pos, sizeof(uint32_t));
        pos += sizeof(uint32_t);

        // number of neighbors
        uint16_t n_nbrs;
        memcpy(&n_nbrs, pos, sizeof(uint16_t));
        pos += sizeof(uint16_t);

        // read neighbor id
        std::vector<long> nbr;
        std::vector<bool> nbr_flag(n_nbrs, false);
        uint32_t n_id;

        for (size_t j=0; j<n_nbrs; ++j)  {
            // read neighbor id
            memcpy(&n_id, pos, sizeof(uint32_t));
            pos += sizeof(uint32_t);
            nbr.push_back(n_id-1); // ogc_fid starts from 1, so -1 here
            nbr_flag[j] = true;
        }
        gl->setNbrIds(nbr);

        // read neighbor weights, if needed
        if (w_size[i] > (size_t)(pos - bw[i])) {
            std::vector<double> nbrWeight;
            float n_weight = 0;
            for (size_t j = 0; j < n_nbrs; ++j) {
                // read neighbor weight
                memcpy(&n_weight, pos, sizeof(float));
                pos += sizeof(float);
                nbrWeight.push_back(n_weight);
            }
            gl->setNbrWeights(nbrWeight);
        }

        int k = fid - 1; // ogc_fid starts from 1, so -1 here
        this->w_dict[k] = gl;
        this->fids.push_back(k);
        this->mask[k] = true;
    }

    this->num_obs = NN;
    //this->GetNbrStats();

    lwdebug(1, "create_weights_from_barray(). sparsity=%f", this->GetSparsity());
}

BinWeight::~BinWeight() {
    // don't clean anything here, since the memory is managed by pg?
}

bool BinWeight::CheckNeighbor(int obs_idx, int nbr_idx) {
    if (w_dict.find(obs_idx) != w_dict.end()) {
        BinElement *el = w_dict[obs_idx];
        return el->checkNeighbor(nbr_idx);
    }
    return false;
}

const std::vector<long> BinWeight::GetNeighbors(int obs_idx) {
    if (w_dict.find(obs_idx) != w_dict.end()) {
        return w_dict[obs_idx]->getNbrIds();
    }
    return std::vector<long>();
}

const std::vector<double> BinWeight::GetNeighborWeights(int obs_idx) {
    if (w_dict.find(obs_idx) != w_dict.end()) {
        return w_dict[obs_idx]->getNbrWeights();
    }
    return std::vector<double>();
}

void BinWeight::Update(const std::vector<bool> &undefs) {
    // unimplemented
}

bool BinWeight::HasIsolates() {

    return false;
}

void BinWeight::GetNbrStats() {
    lwdebug(1, "BinWeight::GetNbrStats() start");
    // sparsity
    double empties = 0;

    boost::unordered_map<uint32_t, BinElement*>::iterator it;

    for (it = w_dict.begin(); it!= w_dict.end(); ++it) {
        if (it->second->getSize() == 0)
            empties += 1;
    }
    sparsity = empties / (double)num_obs;

    // density
    // other
    int i=0, sum_nnbrs = 0;
    std::vector<int> nnbrs_array;
    std::map<int, int> e_dict;

    for (it = w_dict.begin(); it!= w_dict.end(); ++it) {
        int n_nbrs = 0;
        const std::vector<long>& nbrs = it->second->getNbrIds();
        for (size_t j=0; j<nbrs.size();j++) {
            int nbr = nbrs[j];
            if (it->first != nbr) {
                n_nbrs++;
                e_dict[it->first] = nbr;
                e_dict[nbr] = it->first;
            }
        }
        sum_nnbrs += n_nbrs;
        if (i==0 || n_nbrs < min_nbrs) min_nbrs = n_nbrs;
        if (i==0 || n_nbrs > max_nbrs) max_nbrs = n_nbrs;
        nnbrs_array.push_back(n_nbrs);
        i++;
    }

    double n_edges = e_dict.size() / 2.0;
    sparsity = 100.0 * sum_nnbrs / (double)(num_obs * num_obs);

    if (num_obs > 0) mean_nbrs = sum_nnbrs / (double)num_obs;

    std::sort(nnbrs_array.begin(), nnbrs_array.end());

    if (num_obs == nnbrs_array.size()) { // here we has an exception of _fast function
        if (num_obs % 2 == 0) {
            median_nbrs = (nnbrs_array[num_obs / 2 - 1] + nnbrs_array[num_obs / 2]) / 2.0;
        } else {
            median_nbrs = nnbrs_array[num_obs / 2];
        }
    }

    lwdebug(1, "BinWeight::GetNbrStats() end");
}

int BinWeight::GetNbrSize(int obs_idx) {
    if (w_dict.find(obs_idx) != w_dict.end()) {
        BinElement *el = w_dict[obs_idx];
        return el->getSize();
    }
    return 0;
}

double BinWeight::SpatialLag(int obs_idx, const std::vector<double> &data) {
    return 0;
}

const std::vector<uint32_t> &BinWeight::getFids() const {
    return fids;
}
