/**
 * Author: Xun Li <lixun910@gmail.com>
 *
 * Changes:
 * 2021-1-27 Update to use libgeoda 0.0.6
 */

#ifndef __POST_GEODA__
#define __POST_GEODA__

#include <vector>

#include <libgeoda/geofeature.h>
#include <libgeoda/gda_interface.h>
#include <libgeoda/pg/geoms.h>

struct PGWeight;

class PostGeoDa : public AbstractGeoDa {
public:
    enum MapType { point_type, polygon_type, line_type, unknown_type };

    PostGeoDa(int num_obs, const std::vector<uint32_t>& fids);
    virtual ~PostGeoDa();

    // interfaces from AbstractGeoDa
    virtual int GetNumObs() const;
    virtual const std::vector<gda::PointContents*>& GetCentroids();
    virtual int GetMapType();
    virtual std::string GetMapTypeName();
    virtual gda::MainMap& GetMainMap();

    void SetMapType(int geom_type);
    void AddPoint(LWPOINT* lw_pt);
    void AddMultiPoint( LWMPOINT* lw_mpt);
    void AddPolygon( LWPOLY* lw_poly);
    void AddMultiPolygon( LWMPOLY* lw_mpoly);
    void AddNullGeometry();

    PGWeight* create_pgweight(GeoDaWeight* gda_w);

    PGWeight* CreateContWeights(bool is_queen, int order, bool inc_lower, double precision_threshold);

    PGWeight* CreateKnnWeights(int k, double power, bool is_inverse, bool is_arc, bool is_mile,
                               std::string kernel = "", double bandwidth = 0,
                               bool adaptive_bandwidth = false, bool use_kernel_diagonal = false);

    PGWeight* CreateDistanceWeights(double dist_threshold, double power, bool is_inverse, bool is_arc, bool is_mile,
                                    std::string kernel = "", bool use_kernel_diagonal = false);

    double GetMinDistThreshold(bool is_arc, bool is_mile);

protected:
    int map_type;

    int num_obs;

    gda::MainMap main_map;

    std::vector<gda::PointContents*> centroids;

    std::vector<uint32_t> fids;
};


#endif
