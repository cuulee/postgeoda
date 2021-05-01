/**
 * Author: Xun Li <lixun910@gmail.com>
 *
 * The binary format of spatial weights
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
 * Changes:
 * 2021-1-27 Update to use libgeoda 0.0.6
 */

#ifndef __BINWEIGHT__
#define __BINWEIGHT__

#include <vector>
#include <boost/unordered_map.hpp>

#include <libgeoda/weights/GalWeight.h>
#include <libgeoda/weights/GeodaWeight.h>

class BinElement {
protected:
    uint32_t idx;

    std::vector<long> nbr_ids;

    std::vector<double> nbr_weights;

public:
    BinElement();
    virtual ~BinElement();

    void setIdx(uint32_t idx);
    uint32_t getIdx() const;
    void setNbrIds(const std::vector<long> &nbrIds);
    void setNbrWeights(const std::vector<double> &nbrWeights);
    const std::vector<long> &getNbrIds() const;
    const std::vector<double> &getNbrWeights() const;
    bool checkNeighbor(uint32_t nbrIdx);
    size_t getSize();


};

class BinWeight : public GeoDaWeight {
    boost::unordered_map<uint32_t, BinElement*> w_dict;

    std::vector<uint32_t> fids;

public:
    const std::vector<uint32_t> &getFids() const;

public:
    BinWeight() {}
    BinWeight(const uint8_t* bw);
    BinWeight(int N, const uint8_t** bw, const size_t* w_size);

    virtual ~BinWeight();

    virtual bool   CheckNeighbor(int obs_idx, int nbr_idx);
    virtual const  std::vector<long> GetNeighbors(int obs_idx);
    virtual const  std::vector<double> GetNeighborWeights(int obs_idx);
    virtual void   Update(const std::vector<bool>& undefs);
    virtual bool   HasIsolates();
    virtual void   GetNbrStats();

    virtual int    GetNbrSize(int obs_idx);
    virtual double SpatialLag(int obs_idx, const std::vector<double>& data);
    virtual bool   Save(const char* ofname,
                        const char* layer_name,
                        const char* id_var_name,
                        const std::vector<int>& id_vec) { return false;}

    virtual bool   Save(const char* ofname,
                        const char* layer_name,
                        const char* id_var_name,
                        const std::vector<std::string>& id_vec) {return false;}

};
#endif