#ifndef __BINWEIGHT__
#define __BINWEIGHT__

#include <vector>
#include <boost/unordered_map.hpp>

#include <GeodaWeight.h>

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
    virtual bool   HasIsolations();
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
                        const std::vector<const char*>& id_vec) {return false;}

};
#endif