#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <limits.h>

#include <utils/geo_decls.h>

#include "lwgeom_log.h"
#include "geoms.h"

static char *lwgeomTypeName[] =
        {
                "Unknown",
                "Point",
                "LineString",
                "Polygon",
                "MultiPoint",
                "MultiLineString",
                "MultiPolygon",
                "GeometryCollection",
                "CircularString",
                "CompoundCurve",
                "CurvePolygon",
                "MultiCurve",
                "MultiSurface",
                "PolyhedralSurface",
                "Triangle",
                "Tin"
        };

/* Default allocators */
static void * default_allocator(size_t size);
static void default_freeor(void *mem);
static void * default_reallocator(void *mem, size_t size);
lwallocator lwalloc_var = default_allocator;
lwreallocator lwrealloc_var = default_reallocator;
lwfreeor lwfree_var = default_freeor;

/*
 * Default allocators
 *
 * We include some default allocators that use malloc/free/realloc
 * along with stdout/stderr since this is the most common use case
 *
 */

static void *
default_allocator(size_t size)
{
    void *mem = malloc(size);
    return mem;
}

static void *
default_reallocator(void *mem, size_t size)
{
    void *ret = realloc(mem, size);
    return ret;
}

lwflags_t lwflags(int hasz, int hasm, int geodetic)
{
    lwflags_t flags = 0;
    if (hasz)
        FLAGS_SET_Z(flags, 1);
    if (hasm)
        FLAGS_SET_M(flags, 1);
    if (geodetic)
        FLAGS_SET_GEODETIC(flags, 1);
    return flags;
}

const char*
lwtype_name(uint8_t type)
{
    if ( type > 15 )
    {
        /* assert(0); */
        return "Invalid type";
    }
    return lwgeomTypeName[(int ) type];
}

void *
lwalloc(size_t size)
{
    void *mem = lwalloc_var(size);
    //LWDEBUGF(5, "lwalloc: %d@%p", size, mem);
    return mem;
}

void *
lwrealloc(void *mem, size_t size)
{
    //LWDEBUGF(5, "lwrealloc: %d@%p", size, mem);
    return lwrealloc_var(mem, size);
}

void
lwfree(void *mem)
{
    lwfree_var(mem);
}

void
ptarray_free(POINTARRAY *pa)
{
    if (pa)
    {
        if (pa->serialized_pointlist && (!FLAGS_GET_READONLY(pa->flags)))
            lwfree(pa->serialized_pointlist);
        lwfree(pa);
    }
}

void lwpoint_free(LWPOINT *pt)
{
    if ( ! pt ) return;

    if ( pt->bbox )
        lwfree(pt->bbox);
    if ( pt->point )
        ptarray_free(pt->point);
    lwfree(pt);
}

LWPOLY *
lwpoly_construct_empty(int32_t srid, char hasz, char hasm)
{
    LWPOLY *result = lwalloc(sizeof(LWPOLY));
    result->type = POLYGONTYPE;
    result->flags = lwflags(hasz,hasm,0);
    result->srid = srid;
    result->nrings = 0;
    result->maxrings = 1; /* Allocate room for ring, just in case. */
    result->rings = lwalloc(result->maxrings * sizeof(POINTARRAY*));
    result->bbox = NULL;
    return result;
}

void
lwpoly_free(LWPOLY* poly)
{
    uint32_t t;

    if (!poly) return;

    if (poly->bbox) lwfree(poly->bbox);

    if ( poly->rings )
    {
        for (t = 0; t < poly->nrings; t++)
            if (poly->rings[t]) ptarray_free(poly->rings[t]);
        lwfree(poly->rings);
    }

    lwfree(poly);
}

void lwmpoint_free(LWMPOINT *mpt)
{
    uint32_t i;

    if ( ! mpt ) return;

    if ( mpt->bbox )
        lwfree(mpt->bbox);

    for ( i = 0; i < mpt->ngeoms; i++ )
        if ( mpt->geoms && mpt->geoms[i] )
            lwpoint_free(mpt->geoms[i]);

    if ( mpt->geoms )
        lwfree(mpt->geoms);

    lwfree(mpt);
}

void lwmpoly_free(LWMPOLY *mpoly)
{
    uint32_t i;
    if ( ! mpoly ) return;
    if ( mpoly->bbox )
        lwfree(mpoly->bbox);

    for ( i = 0; i < mpoly->ngeoms; i++ )
        if ( mpoly->geoms && mpoly->geoms[i] )
            lwpoly_free(mpoly->geoms[i]);

    if ( mpoly->geoms )
        lwfree(mpoly->geoms);

    lwfree(mpoly);
}

void lwcollection_free(LWCOLLECTION *col)
{
    uint32_t i;
    if ( ! col ) return;

    if ( col->bbox )
    {
        lwfree(col->bbox);
    }
    for ( i = 0; i < col->ngeoms; i++ )
    {
        LWDEBUGF(4,"freeing geom[%d]", i);
        if ( col->geoms && col->geoms[i] )
            lwgeom_free(col->geoms[i]);
    }
    if ( col->geoms )
    {
        lwfree(col->geoms);
    }
    lwfree(col);
}

void lwgeom_free(LWGEOM *lwgeom)
{

    /* There's nothing here to free... */
    if( ! lwgeom ) return;

    LWDEBUGF(5,"freeing a %s",lwtype_name(lwgeom->type));

    switch (lwgeom->type)
    {
        case POINTTYPE:
            lwpoint_free((LWPOINT *)lwgeom);
            break;

        case POLYGONTYPE:
            lwpoly_free((LWPOLY *)lwgeom);
            break;

        case MULTIPOINTTYPE:
            lwmpoint_free((LWMPOINT *)lwgeom);
            break;

        case MULTIPOLYGONTYPE:
            lwmpoly_free((LWMPOLY *)lwgeom);
            break;

        case CURVEPOLYTYPE:
        case COMPOUNDTYPE:
        case MULTICURVETYPE:
        case MULTISURFACETYPE:
        case COLLECTIONTYPE:
            lwcollection_free((LWCOLLECTION *)lwgeom);
            break;
        case LINETYPE:
        case CIRCSTRINGTYPE:
        case TRIANGLETYPE:
        case MULTILINETYPE:
        case POLYHEDRALSURFACETYPE:
        case TINTYPE:
            lwerror("lwgeom_free called with unsupported type (%d) %s", lwgeom->type, lwtype_name(lwgeom->type));
        default:
            lwerror("lwgeom_free called with unknown type (%d) %s", lwgeom->type, lwtype_name(lwgeom->type));
    }
    return;
}

POINTARRAY*
ptarray_construct(char hasz, char hasm, uint32_t npoints)
{
    POINTARRAY *pa = ptarray_construct_empty(hasz, hasm, npoints);
    pa->npoints = npoints;
    return pa;
}

POINTARRAY*
ptarray_construct_empty(char hasz, char hasm, uint32_t maxpoints)
{
    POINTARRAY *pa = lwalloc(sizeof(POINTARRAY));
    pa->serialized_pointlist = NULL;

    /* Set our dimensionality info on the bitmap */
    pa->flags = lwflags(hasz, hasm, 0);

    /* We will be allocating a bit of room */
    pa->npoints = 0;
    pa->maxpoints = maxpoints;

    /* Allocate the coordinate array */
    if ( maxpoints > 0 )
        pa->serialized_pointlist = lwalloc(maxpoints * ptarray_point_size(pa));
    else
        pa->serialized_pointlist = NULL;

    return pa;
}

POINTARRAY*
ptarray_construct_copy_data(char hasz, char hasm, uint32_t npoints, const uint8_t *ptlist)
{
    POINTARRAY *pa = lwalloc(sizeof(POINTARRAY));

    pa->flags = lwflags(hasz, hasm, 0);
    pa->npoints = npoints;
    pa->maxpoints = npoints;

    if ( npoints > 0 )
    {
        pa->serialized_pointlist = lwalloc(ptarray_point_size(pa) * npoints);
        memcpy(pa->serialized_pointlist, ptlist, ptarray_point_size(pa) * npoints);
    }
    else
    {
        pa->serialized_pointlist = NULL;
    }

    return pa;
}

/*
 * Construct a new point.  point will not be copied
 * use SRID=SRID_UNKNOWN for unknown SRID (will have 8bit type's S = 0)
 */
LWPOINT *
lwpoint_construct(int32_t srid, GBOX *bbox, POINTARRAY *point)
{
    LWPOINT *result;
    lwflags_t flags = 0;

    if (point == NULL)
        return NULL; /* error */

    result = lwalloc(sizeof(LWPOINT));
    result->type = POINTTYPE;
    FLAGS_SET_Z(flags, FLAGS_GET_Z(point->flags));
    FLAGS_SET_M(flags, FLAGS_GET_M(point->flags));
    FLAGS_SET_BBOX(flags, bbox?1:0);
    result->flags = flags;
    result->srid = srid;
    result->point = point;
    result->bbox = bbox;

    return result;
}

LWPOINT *
lwpoint_construct_empty(int32_t srid, char hasz, char hasm)
{
    LWPOINT *result = lwalloc(sizeof(LWPOINT));
    result->type = POINTTYPE;
    result->flags = lwflags(hasz, hasm, 0);
    result->srid = srid;
    result->point = ptarray_construct(hasz, hasm, 0);
    result->bbox = NULL;
    return result;
}


/**
* Byte
* Read a byte and advance the parse state forward.
*/
static char byte_from_wkb_state(wkb_parse_state *s)
{
    char char_value = 0;
    //LWDEBUG(4, "Entered function");

    wkb_parse_state_check(s, WKB_BYTE_SIZE);
    if (s->error)
        return 0;
    //LWDEBUG(4, "Passed state check");

    char_value = s->pos[0];
    //LWDEBUGF(4, "Read byte value: %x", char_value);
    s->pos += WKB_BYTE_SIZE;

    return char_value;
}

/**
* Int32
* Read 4-byte integer and advance the parse state forward.
*/
static uint32_t integer_from_wkb_state(wkb_parse_state *s)
{
    uint32_t i = 0;

    wkb_parse_state_check(s, WKB_INT_SIZE);
    if (s->error)
        return 0;

    memcpy(&i, s->pos, WKB_INT_SIZE);

    /* Swap? Copy into a stack-allocated integer. */
    if( s->swap_bytes )
    {
        int j = 0;
        uint8_t tmp;

        for( j = 0; j < WKB_INT_SIZE/2; j++ )
        {
            tmp = ((uint8_t*)(&i))[j];
            ((uint8_t*)(&i))[j] = ((uint8_t*)(&i))[WKB_INT_SIZE - j - 1];
            ((uint8_t*)(&i))[WKB_INT_SIZE - j - 1] = tmp;
        }
    }

    s->pos += WKB_INT_SIZE;
    return i;
}

/**
* Double
* Read an 8-byte double and advance the parse state forward.
*/
static double double_from_wkb_state(wkb_parse_state *s)
{
    double d = 0;

    memcpy(&d, s->pos, WKB_DOUBLE_SIZE);

    /* Swap? Copy into a stack-allocated integer. */
    if( s->swap_bytes )
    {
        int i = 0;
        uint8_t tmp;

        for( i = 0; i < WKB_DOUBLE_SIZE/2; i++ )
        {
            tmp = ((uint8_t*)(&d))[i];
            ((uint8_t*)(&d))[i] = ((uint8_t*)(&d))[WKB_DOUBLE_SIZE - i - 1];
            ((uint8_t*)(&d))[WKB_DOUBLE_SIZE - i - 1] = tmp;
        }

    }

    s->pos += WKB_DOUBLE_SIZE;
    return d;
}

/**
* Take in an unknown kind of wkb type number and ensure it comes out
* as an extended WKB type number (with Z/M/SRID flags masked onto the
* high bits).
*/
static void lwtype_from_wkb_state(wkb_parse_state *s, uint32_t wkb_type)
{
    uint32_t wkb_simple_type;

    //LWDEBUG(4, "Entered function");

    s->has_z = LW_FALSE;
    s->has_m = LW_FALSE;
    s->has_srid = LW_FALSE;

    /* If any of the higher bits are set, this is probably an extended type. */
    if( wkb_type & 0xF0000000 )
    {
        if( wkb_type & WKBZOFFSET ) s->has_z = LW_TRUE;
        if( wkb_type & WKBMOFFSET ) s->has_m = LW_TRUE;
        if( wkb_type & WKBSRIDFLAG ) s->has_srid = LW_TRUE;
        //LWDEBUGF(4, "Extended type: has_z=%d has_m=%d has_srid=%d", s->has_z, s->has_m, s->has_srid);
    }

    /* Mask off the flags */
    wkb_type = wkb_type & 0x0FFFFFFF;
    /* Strip out just the type number (1-12) from the ISO number (eg 3001-3012) */
    wkb_simple_type = wkb_type % 1000;

    /* Extract the Z/M information from ISO style numbers */
    if( wkb_type >= 3000 && wkb_type < 4000 )
    {
        s->has_z = LW_TRUE;
        s->has_m = LW_TRUE;
    }
    else if ( wkb_type >= 2000 && wkb_type < 3000 )
    {
        s->has_m = LW_TRUE;
    }
    else if ( wkb_type >= 1000 && wkb_type < 2000 )
    {
        s->has_z = LW_TRUE;
    }

    switch (wkb_simple_type)
    {
        case WKB_POINT_TYPE:
            s->lwtype = POINTTYPE;
            break;
        case WKB_LINESTRING_TYPE:
            s->lwtype = LINETYPE;
            break;
        case WKB_POLYGON_TYPE:
            s->lwtype = POLYGONTYPE;
            break;
        case WKB_MULTIPOINT_TYPE:
            s->lwtype = MULTIPOINTTYPE;
            break;
        case WKB_MULTILINESTRING_TYPE:
            s->lwtype = MULTILINETYPE;
            break;
        case WKB_MULTIPOLYGON_TYPE:
            s->lwtype = MULTIPOLYGONTYPE;
            break;
        case WKB_GEOMETRYCOLLECTION_TYPE:
            s->lwtype = COLLECTIONTYPE;
            break;
        case WKB_CIRCULARSTRING_TYPE:
            s->lwtype = CIRCSTRINGTYPE;
            break;
        case WKB_COMPOUNDCURVE_TYPE:
            s->lwtype = COMPOUNDTYPE;
            break;
        case WKB_CURVEPOLYGON_TYPE:
            s->lwtype = CURVEPOLYTYPE;
            break;
        case WKB_MULTICURVE_TYPE:
            s->lwtype = MULTICURVETYPE;
            break;
        case WKB_MULTISURFACE_TYPE:
            s->lwtype = MULTISURFACETYPE;
            break;
        case WKB_POLYHEDRALSURFACE_TYPE:
            s->lwtype = POLYHEDRALSURFACETYPE;
            break;
        case WKB_TIN_TYPE:
            s->lwtype = TINTYPE;
            break;
        case WKB_TRIANGLE_TYPE:
            s->lwtype = TRIANGLETYPE;
            break;

            /* PostGIS 1.5 emits 13, 14 for CurvePolygon, MultiCurve */
            /* These numbers aren't SQL/MM (numbers currently only */
            /* go up to 12. We can handle the old data here (for now??) */
            /* converting them into the lwtypes that are intended. */
        case WKB_CURVE_TYPE:
            s->lwtype = CURVEPOLYTYPE;
            break;
        case WKB_SURFACE_TYPE:
            s->lwtype = MULTICURVETYPE;
            break;

        default: /* Error! */
            //lwerror("Unknown WKB type (%d)! Full WKB type number was (%d).", wkb_simple_type, wkb_type);
            break;
    }

    //LWDEBUGF(4,"Got lwtype %s (%u)", lwtype_name(s->lwtype), s->lwtype);

    return;
}

/**
* POINTARRAY
* Read a dynamically sized point array and advance the parse state forward.
* First read the number of points, then read the points.
*/
static POINTARRAY* ptarray_from_wkb_state(wkb_parse_state *s)
{
    POINTARRAY *pa = NULL;
    size_t pa_size;
    uint32_t ndims = 2;
    uint32_t npoints = 0;
    static uint32_t maxpoints = UINT_MAX / WKB_DOUBLE_SIZE / 4;

    /* Calculate the size of this point array. */
    npoints = integer_from_wkb_state(s);
    if (s->error)
        return NULL;
    if (npoints > maxpoints)
    {
        lwerror("Pointarray length (%d) is too large");
        return NULL;
    }

    LWDEBUGF(4,"Pointarray has %d points", npoints);

    if( s->has_z ) ndims++;
    if( s->has_m ) ndims++;
    pa_size = npoints * ndims * WKB_DOUBLE_SIZE;

    /* Empty! */
    if( npoints == 0 )
        return ptarray_construct(s->has_z, s->has_m, npoints);

    /* Does the data we want to read exist? */
    wkb_parse_state_check(s, pa_size);
    if (s->error)
        return NULL;

    /* If we're in a native endianness, we can just copy the data directly! */
    if( ! s->swap_bytes )
    {
        pa = ptarray_construct_copy_data(s->has_z, s->has_m, npoints, (uint8_t*)s->pos);
        s->pos += pa_size;
    }
        /* Otherwise we have to read each double, separately. */
    else
    {
        uint32_t i = 0;
        double *dlist;
        pa = ptarray_construct(s->has_z, s->has_m, npoints);
        dlist = (double*)(pa->serialized_pointlist);
        for( i = 0; i < npoints * ndims; i++ )
        {
            dlist[i] = double_from_wkb_state(s);
        }
    }

    return pa;
}

/**
* POINT
* Read a WKB point, starting just after the endian byte,
* type number and optional srid number.
* Advance the parse state forward appropriately.
* WKB point has just a set of doubles, with the quantity depending on the
* dimension of the point, so this looks like a special case of the above
* with only one point.
*/
static LWPOINT* lwpoint_from_wkb_state(wkb_parse_state *s)
{
    static uint32_t npoints = 1;
    POINTARRAY *pa = NULL;
    size_t pa_size;
    uint32_t ndims = 2;
    const POINT2D *pt;

    /* Count the dimensions. */
    if( s->has_z ) ndims++;
    if( s->has_m ) ndims++;
    pa_size = ndims * WKB_DOUBLE_SIZE;

    /* Does the data we want to read exist? */
    wkb_parse_state_check(s, pa_size);
    if (s->error)
        return NULL;

    /* If we're in a native endianness, we can just copy the data directly! */
    if( ! s->swap_bytes )
    {
        pa = ptarray_construct_copy_data(s->has_z, s->has_m, npoints, (uint8_t*)s->pos);
        s->pos += pa_size;
    }
        /* Otherwise we have to read each double, separately */
    else
    {
        uint32_t i = 0;
        double *dlist;
        pa = ptarray_construct(s->has_z, s->has_m, npoints);
        dlist = (double*)(pa->serialized_pointlist);
        for( i = 0; i < ndims; i++ )
        {
            dlist[i] = double_from_wkb_state(s);
        }
    }

    /* Check for POINT(NaN NaN) ==> POINT EMPTY */
    pt = getPoint2d_cp(pa, 0);
    if ( isnan(pt->x) && isnan(pt->y) )
    {
        ptarray_free(pa);
        return lwpoint_construct_empty(s->srid, s->has_z, s->has_m);
    }
    else
    {
        return lwpoint_construct(s->srid, NULL, pa);
    }
}

/**
* POLYGON
* Read a WKB polygon, starting just after the endian byte,
* type number and optional srid number. Advance the parse state
* forward appropriately.
* First read the number of rings, then read each ring
* (which are structured as point arrays)
*/
static LWPOLY* lwpoly_from_wkb_state(wkb_parse_state *s)
{
    uint32_t nrings = integer_from_wkb_state(s);
    if (s->error)
        return NULL;
    uint32_t i = 0;
    LWPOLY *poly = lwpoly_construct_empty(s->srid, s->has_z, s->has_m);

    LWDEBUGF(4,"Polygon has %d rings", nrings);

    /* Empty polygon? */
    if( nrings == 0 )
        return poly;

    for( i = 0; i < nrings; i++ )
    {
        POINTARRAY *pa = ptarray_from_wkb_state(s);
        if (pa == NULL)
        {
            lwpoly_free(poly);
            return NULL;
        }

        /* Check for at least four points. */
        if (s->check & LW_PARSER_CHECK_MINPOINTS && pa->npoints < 4)
        {
            lwpoly_free(poly);
            LWDEBUGF(2, "%s must have at least four points in each ring", lwtype_name(s->lwtype));
            lwerror("%s must have at least four points in each ring", lwtype_name(s->lwtype));
            return NULL;
        }

        /* Check that first and last points are the same. */
        if( s->check & LW_PARSER_CHECK_CLOSURE && ! ptarray_is_closed_2d(pa) )
        {
            lwpoly_free(poly);
            LWDEBUGF(2, "%s must have closed rings", lwtype_name(s->lwtype));
            lwerror("%s must have closed rings", lwtype_name(s->lwtype));
            return NULL;
        }

        /* Add ring to polygon */
        if ( lwpoly_add_ring(poly, pa) == LW_FAILURE )
        {
            lwpoly_free(poly);
            LWDEBUG(2, "Unable to add ring to polygon");
            lwerror("Unable to add ring to polygon");
            return NULL;
        }

    }
    return poly;
}

/**
* GEOMETRY
* Generic handling for WKB geometries. The front of every WKB geometry
* (including those embedded in collections) is an endian byte, a type
* number and an optional srid number. We handle all those here, then pass
* to the appropriate handler for the specific type.
*/
LWGEOM* lwgeom_from_wkb_state(wkb_parse_state *s)
{
    char wkb_little_endian;
    uint32_t wkb_type;

    //LWDEBUG(4,"Entered function");

    /* Fail when handed incorrect starting byte */
    wkb_little_endian = byte_from_wkb_state(s);
    if (s->error)
        return NULL;
    if( wkb_little_endian != 1 && wkb_little_endian != 0 )
    {
        LWDEBUG(4,"Leaving due to bad first byte!");
        //lwerror("Invalid endian flag value encountered.");
        return NULL;
    }

    /* Check the endianness of our input  */
    s->swap_bytes = LW_FALSE;

    /* Machine arch is big endian, request is for little */
    if (IS_BIG_ENDIAN && wkb_little_endian)
        s->swap_bytes = LW_TRUE;
        /* Machine arch is little endian, request is for big */
    else if ((!IS_BIG_ENDIAN) && (!wkb_little_endian))
        s->swap_bytes = LW_TRUE;

    /* Read the type number */
    wkb_type = integer_from_wkb_state(s);
    if (s->error)
        return NULL;
    LWDEBUGF(4,"Got WKB type number: 0x%X", wkb_type);
    lwtype_from_wkb_state(s, wkb_type);

    /* Read the SRID, if necessary */
    if( s->has_srid )
    {
        s->srid = clamp_srid(integer_from_wkb_state(s));
        if (s->error)
            return NULL;
        /* TODO: warn on explicit UNKNOWN srid ? */
        LWDEBUGF(4,"Got SRID: %u", s->srid);
    }

    /* Do the right thing */
    switch( s->lwtype )
    {
        case POINTTYPE:
            return (LWGEOM*)lwpoint_from_wkb_state(s);
            break;

        case POLYGONTYPE:
            return (LWGEOM*)lwpoly_from_wkb_state(s);
            break;

        case CURVEPOLYTYPE:
            return (LWGEOM*)lwcurvepoly_from_wkb_state(s);
            break;
        case MULTIPOINTTYPE:
        case MULTILINETYPE:
        case MULTIPOLYGONTYPE:
        case COMPOUNDTYPE:
        case MULTICURVETYPE:
        case MULTISURFACETYPE:
        case POLYHEDRALSURFACETYPE:
        case TINTYPE:
        case COLLECTIONTYPE:
            return (LWGEOM*)lwcollection_from_wkb_state(s);
            break;
        case LINETYPE:
            //return (LWGEOM*)lwline_from_wkb_state(s);
            break;
        case CIRCSTRINGTYPE:
            //return (LWGEOM*)lwcircstring_from_wkb_state(s);
            break;
        case TRIANGLETYPE:
            //return (LWGEOM*)lwtriangle_from_wkb_state(s);
            break;
            /* Unknown type! */
        default:
            lwerror("%s: Unsupported geometry type: %s", __func__, lwtype_name(s->lwtype));
    }

    /* Return value to keep compiler happy. */
    return NULL;

}