/**
 * Author: Xun Li <lixun910@gmail.com>
 *
 * Changes:
 * 2021-1-27 Update to use libgeoda 0.0.6
 */

#include <libgeoda/shape/centroid.h>
#include <libgeoda/gda_weights.h>
#include <libgeoda/weights/GeodaWeight.h>
#include <libgeoda/geofeature.h>
#include <libgeoda/pg/utils.h>

#include "postgeoda.h"
#include "proxy.h"


PostGeoDa::PostGeoDa(int num_obs, const std::vector<uint32_t>& _fids)
{
    this->map_type = gda::NULL_SHAPE;
    this->num_obs = num_obs;
    this->fids = _fids;
}

PostGeoDa::~PostGeoDa()
{

}

int PostGeoDa::GetNumObs() const {
    return this->num_obs;
}

const std::vector<gda::PointContents*> & PostGeoDa::GetCentroids() {
    lwdebug(1, "Enter PostGeoDa::GetCentroids.");
    if (this->centroids.empty()) {
        if (this->GetMapType() == gda::POINT_TYP) {
            this->centroids.resize(this->GetNumObs());
            for (size_t i=0; i<this->centroids.size(); ++i) {
                this->centroids[i] = (gda::PointContents*)this->main_map.records[i];
            }
        } else if (this->GetMapType() == gda::POLYGON) {
            this->centroids.resize(this->GetNumObs());
            for (size_t i=0; i<this->centroids.size(); ++i) {
                gda::PolygonContents* poly = (gda::PolygonContents*)this->main_map.records[i];
                Centroid cent(poly);
                this->centroids[i] = new gda::PointContents;
                cent.getCentroid(*this->centroids[i]);
            }
        } else {
            lwerror("Enter PostGeoDa::GetCentroids() shape_type=%d not correct.", this->main_map.shape_type);
        }
    }
    return this->centroids;
}

int PostGeoDa::GetMapType() {
    return this->map_type;
}

std::string PostGeoDa::GetMapTypeName()
{
    if (this->GetMapType() == gda::POINT_TYP) {
        return "point";
    } else if (this->GetMapType() == gda::POLYGON) {
        return "polygon";
    } else {
        return "not supported";
    }
}

gda::MainMap & PostGeoDa::GetMainMap() {
    return this->main_map;
}

void PostGeoDa::SetMapType(int geom_type)
{
    //if (geom_type == POINTTYPE || geom_type == MULTIPOINTTYPE) {
    if (geom_type == 1 || geom_type == 4) {
        this->map_type = gda::POINT_TYP;
    } else if (geom_type == POLYGONTYPE || geom_type == MULTIPOLYGONTYPE) {
        this->map_type = gda::POLYGON;
    } else {
        lwerror("PostGeoDa::SetMapType() geom_type=%d", geom_type);
    }
}

void PostGeoDa::AddPoint(LWPOINT *lw_pt) {
    /* Grab the point: note getPoint4d will correctly handle
	the case where the POINTs don't contain Z or M coordinates */
    POINT4D p4d;
    p4d = getPoint4d(lw_pt->point, 0);

    gda::PointContents* pt = new gda::PointContents();
    pt->x = p4d.x;
    pt->y = p4d.y;
    this->main_map.set_bbox(pt->x,  pt->y);
    this->main_map.records.push_back(pt);
}

void PostGeoDa::AddMultiPoint(LWMPOINT *lw_mpt) {
    /* Grab the points: note getPoint4d will correctly handle
	the case where the POINTs don't contain Z or M coordinates */
    POINT4D p4d;
    for (size_t i = 0; i < lw_mpt->ngeoms; i++)
    {
        p4d = getPoint4d(lw_mpt->geoms[i]->point, 0);

        gda::PointContents* pt = new gda::PointContents();
        pt->x = p4d.x;
        pt->y = p4d.y;
        this->main_map.set_bbox(pt->x,  pt->y);
        this->main_map.records.push_back(pt);
        break;// only take the first point, even it has multipoints
    }
}

void PostGeoDa::AddPolygon(LWPOLY *lw_poly) {
    size_t  i, j;
    POINT4D p4d;
    /* Allocate storage for ring pointers */
    int shppointtotal = 0, shppoint = 0;

    /* First count through all the points in each ring so we now how much memory is required */
    for (i = 0; i < lw_poly->nrings; i++)
        shppointtotal += lw_poly->rings[i]->npoints;


    gda::PolygonContents *poly = new gda::PolygonContents();
    poly->num_parts = 0;
    poly->num_points = 0;

    double minx = std::numeric_limits<double>::max();
    double miny = std::numeric_limits<double>::max();
    double maxx = std::numeric_limits<double>::lowest();
    double maxy = std::numeric_limits<double>::lowest();
    double x, y;

    /* Iterate through each ring setting up shpparts to point to the beginning of each ring */
    for (i = 0; i < lw_poly->nrings; i++) {
        /* For each ring, store the integer coordinate offset for the start of each ring */
        poly->num_parts += 1;
        poly->parts.push_back(shppoint);

        bool is_hole = i > 0 ? true : false;
        poly->holes.push_back(is_hole);

        for (j = 0; j < lw_poly->rings[i]->npoints; j++) {
            p4d = getPoint4d(lw_poly->rings[i], j);

            x = p4d.x;
            y = p4d.y;

            poly->points.push_back(gda::Point(x,y));
            poly->num_points += 1;

            if ( x < minx ) minx = x;
            if ( x >= maxx ) maxx = x;
            if ( y < miny ) miny = y;
            if ( y >= maxy ) maxy = y;

            /* Increment the point counter */
            shppoint++;
        }

        /*
		 * First ring should be clockwise,
		 * other rings should be counter-clockwise
		 */
    }

    poly->box.resize(4);
    poly->box[0] = minx;
    poly->box[1] = miny;
    poly->box[2] = maxx;
    poly->box[3] = maxy;

    this->main_map.set_bbox(minx, miny);
    this->main_map.set_bbox(maxx, maxy);
    this->main_map.records.push_back(poly);
}

void PostGeoDa::AddMultiPolygon(LWMPOLY *lw_mpoly) {
    POINT4D p4d;
    uint32_t i, j, k;

    int shppointtotal = 0, shppoint = 0, shpringtotal = 0, shpring = 0;

    /* NOTE: Multipolygons are stored in shapefiles as Polygon* shapes with multiple outer rings */

    /* First count through each ring of each polygon so we now know much memory is required */
    for (i = 0; i < lw_mpoly->ngeoms; i++) {
        for (j = 0; j < lw_mpoly->geoms[i]->nrings; j++) {
            shpringtotal++;
            shppointtotal += lw_mpoly->geoms[i]->rings[j]->npoints;
        }
    }

    gda::PolygonContents *poly = new gda::PolygonContents();
    poly->num_parts = 0;
    poly->num_points = 0;

    double minx = std::numeric_limits<double>::max();
    double miny = std::numeric_limits<double>::max();
    double maxx = std::numeric_limits<double>::lowest();
    double maxy = std::numeric_limits<double>::lowest();
    double x, y;

    /* Iterate through each ring of each polygon in turn */
    for (i = 0; i < lw_mpoly->ngeoms; i++) {
        for (j = 0; j < lw_mpoly->geoms[i]->nrings; j++) {
            /* For each ring, store the integer coordinate offset for the start of each ring */
            poly->parts.push_back(shppoint);
            poly->num_parts += 1;

            bool is_hole = j > 0 ? true : false;
            poly->holes.push_back(is_hole);

            lwdebug(4, "Ring offset: %d", shpring);

            for (k = 0; k < lw_mpoly->geoms[i]->rings[j]->npoints; k++) {
                p4d = getPoint4d(lw_mpoly->geoms[i]->rings[j], k);

                x = p4d.x;
                y = p4d.y;

                poly->points.push_back(gda::Point(x, y));
                poly->num_points += 1;

                if (x < minx) minx = x;
                if (x >= maxx) maxx = x;
                if (y < miny) miny = y;
                if (y >= maxy) maxy = y;

                /* Increment the point counter */
                shppoint++;
            }
            /*
			* First ring should be clockwise,
			* other rings should be counter-clockwise
			*/
        }
        /* Increment the ring counter */
        shpring++;
    }

    lwdebug(4, "poly->box[0]=%f", minx);
    lwdebug(4, "poly->box[1]=%f", miny);
    lwdebug(4, "poly->box[2]=%f", maxx);
    lwdebug(4, "poly->box[3]=%f", maxy);
    poly->box.resize(4);
    poly->box[0] = minx;
    poly->box[1] = miny;
    poly->box[2] = maxx;
    poly->box[3] = maxy;

    this->main_map.set_bbox(minx, miny);
    this->main_map.set_bbox(maxx, maxy);
    this->main_map.records.push_back(poly);
}

void PostGeoDa::AddNullGeometry() {
    this->main_map.records.push_back(new gda::NullShapeContents());
}


/**
 * create_pgweight (internal function)
 * Convert from GeoDaWeight (C++ class) to PGWeight (C struct) that can be used by weights.c
 *
 * @param gda_w
 * @return PGWeight*
 */
PGWeight *PostGeoDa::create_pgweight(GeoDaWeight* gda_w)
{
    PGWeight* pg_w = (PGWeight*)malloc(sizeof(PGWeight));

    pg_w->w_type = 'a'; // GAL type
    if (gda_w->weight_type == GeoDaWeight::gwt_type) pg_w->w_type = 'w';

    pg_w->num_obs = this->num_obs;
    pg_w->neighbors = (PGNeighbor*)malloc(num_obs * sizeof(PGNeighbor));

    for (size_t i=0; i<this->num_obs; i++) {
        int nbr_sz = gda_w->GetNbrSize(i);
        const  std::vector<long>& nbrs = gda_w->GetNeighbors(i);
        const std::vector<double>& nbr_ws = gda_w->GetNeighborWeights(i);

        PGNeighbor* pg_nbr = &pg_w->neighbors[i];

        pg_nbr->idx = fids[i];
        pg_nbr->num_nbrs = gda_w->GetNbrSize(i);
        pg_nbr->nbrId = (uint32_t*)malloc(nbr_sz * sizeof(uint32_t));
        if (pg_w->w_type == 'w')
            pg_nbr->nbrWeight = (float*)malloc(nbr_sz * sizeof(float));

        for (size_t j=0; j<nbr_sz; j++) {
            pg_nbr->nbrId[j] = fids[ nbrs[j] ] ;
            if (pg_w->w_type == 'w')
                pg_nbr->nbrWeight[j] = nbr_ws[j];
        }
        lwdebug(2, "Enter PostGeoDa::create_pgweight() %d-th has %d nn",
                fids[i], nbr_sz);
    }
    return pg_w;
}


PGWeight *PostGeoDa::CreateKnnWeights(int k) {
    lwdebug(1, "Enter PostGeoDa::CreateKnnWeights(k=%d).", k);

    double power = 1.0;
    bool is_inverse = false;
    bool is_arc = false;
    bool is_mile = true;
    std::string kernel = "";
    double bandwidth = 0;
    bool adaptive_bandwidth = false;
    bool use_kernel_diagonal = false;
    std::string poly_id = "";
    GeoDaWeight* gda_w = gda_knn_weights(this, k, power, is_inverse, is_arc, is_mile,
                                         kernel, bandwidth, adaptive_bandwidth, use_kernel_diagonal, poly_id);

    lwdebug(1, "Enter CreateKnnWeights: min_nbrs=%d", gda_w->GetMinNbrs());
    lwdebug(1, "Enter CreateKnnWeights: sparsity=%f", gda_w->GetSparsity());
    lwdebug(1, "GeoDaWeight: gda_w=%x", gda_w);

    PGWeight* pg_w =  create_pgweight(gda_w);

    delete gda_w;

    lwdebug(1, "Enter PostGeoDa::CreateKnnWeights().");
    return pg_w;
}


PGWeight *PostGeoDa::CreateContWeights(bool is_queen, int order, bool inc_lower, double precision_threshold) {
    lwdebug(1, "Enter PostGeoDa::CreateQueenWeights().");
    double shp_min_x = (double)this->main_map.bbox_x_min;
    double shp_max_x = (double)this->main_map.bbox_x_max;
    double shp_min_y = (double)this->main_map.bbox_y_min;
    double shp_max_y = (double)this->main_map.bbox_y_max;

    lwdebug(3, "Enter PostGeoDa::CreateQueenWeights() shp_min_x=%f", shp_min_x);
    lwdebug(3, "Enter PostGeoDa::CreateQueenWeights() shp_max_x=%f", shp_max_x);
    lwdebug(3, "Enter PostGeoDa::CreateQueenWeights() shp_min_y=%f", shp_min_y);
    lwdebug(3, "Enter PostGeoDa::CreateQueenWeights() shp_max_y=%f", shp_max_y);

    GeoDaWeight* gda_w = 0;

    if (is_queen) {
        gda_w = gda_queen_weights(this, order, inc_lower, precision_threshold);
    } else {
        gda_w = gda_rook_weights(this, order, inc_lower, precision_threshold);
    }

    lwdebug(1, "Enter CreateQueenWeights: min_nbrs=%d", gda_w->GetMinNbrs());
    lwdebug(1, "Enter CreateQueenWeights: sparsity=%f", gda_w->GetSparsity());
    lwdebug(1, "GeoDaWeight: gda_w=%x", gda_w);

    PGWeight* pg_w = create_pgweight(gda_w);

    delete gda_w;

    lwdebug(1, "Exit CreateQueenWeights");
    return pg_w;
}