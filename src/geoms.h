#ifndef __POST_GEOMS_H__
#define __POST_GEOMS_H__

#include <stdint.h>

/**
* LWTYPE numbers, used internally by PostGIS
*/
#define POINTTYPE                1
#define LINETYPE                 2
#define POLYGONTYPE              3
#define MULTIPOINTTYPE           4
#define MULTILINETYPE            5
#define MULTIPOLYGONTYPE         6
#define COLLECTIONTYPE           7
#define CIRCSTRINGTYPE           8
#define COMPOUNDTYPE             9
#define CURVEPOLYTYPE           10
#define MULTICURVETYPE          11
#define MULTISURFACETYPE        12
#define POLYHEDRALSURFACETYPE   13
#define TRIANGLETYPE            14
#define TINTYPE                 15
#define NUMTYPES                16

/**
* Macros for manipulating the 'flags' byte. A uint8_t used as follows:
* VVSRGBMZ
* Version bit, followed by
* Validty, Solid, ReadOnly, Geodetic, HasBBox, HasM and HasZ flags.
*/
#define LWFLAG_Z        0x01
#define LWFLAG_M        0x02
#define LWFLAG_BBOX     0x04
#define LWFLAG_GEODETIC 0x08
#define LWFLAG_READONLY 0x10
#define LWFLAG_SOLID    0x20

#define FLAGS_GET_Z(flags)         ((flags) & LWFLAG_Z)
#define FLAGS_GET_M(flags)        (((flags) & LWFLAG_M)>>1)
#define FLAGS_GET_BBOX(flags)     (((flags) & LWFLAG_BBOX)>>2)
#define FLAGS_GET_GEODETIC(flags) (((flags) & LWFLAG_GEODETIC)>>3)
#define FLAGS_GET_READONLY(flags) (((flags) & LWFLAG_READONLY)>>4)
#define FLAGS_GET_SOLID(flags)    (((flags) & LWFLAG_SOLID)>>5)

#define FLAGS_SET_Z(flags, value) ((flags) = (value) ? ((flags) | LWFLAG_Z) : ((flags) & ~LWFLAG_Z))
#define FLAGS_SET_M(flags, value) ((flags) = (value) ? ((flags) | LWFLAG_M) : ((flags) & ~LWFLAG_M))
#define FLAGS_SET_BBOX(flags, value) ((flags) = (value) ? ((flags) | LWFLAG_BBOX) : ((flags) & ~LWFLAG_BBOX))
#define FLAGS_SET_GEODETIC(flags, value) ((flags) = (value) ? ((flags) | LWFLAG_GEODETIC) : ((flags) & ~LWFLAG_GEODETIC))
#define FLAGS_SET_READONLY(flags, value) ((flags) = (value) ? ((flags) | LWFLAG_READONLY) : ((flags) & ~LWFLAG_READONLY))
#define FLAGS_SET_SOLID(flags, value) ((flags) = (value) ? ((flags) | LWFLAG_SOLID) : ((flags) & ~LWFLAG_SOLID))

#define FLAGS_NDIMS(flags) (2 + FLAGS_GET_Z(flags) + FLAGS_GET_M(flags))
#define FLAGS_GET_ZM(flags) (FLAGS_GET_M(flags) + FLAGS_GET_Z(flags) * 2)
#define FLAGS_NDIMS_BOX(flags) (FLAGS_GET_GEODETIC(flags) ? 3 : FLAGS_NDIMS(flags))

/**
* Macro for reading the size from the GSERIALIZED size attribute.
* Cribbed from PgSQL, top 30 bits are size. Use VARSIZE() when working
* internally with PgSQL. See SET_VARSIZE_4B / VARSIZE_4B in
* PGSRC/src/include/postgres.h for details.
*/
#ifdef WORDS_BIGENDIAN
#define SIZE_GET(varsize) ((varsize) & 0x3FFFFFFF)
#define SIZE_SET(varsize, len) ((varsize) = ((len) & 0x3FFFFFFF))
#define IS_BIG_ENDIAN 1
#else
#define SIZE_GET(varsize) (((varsize) >> 2) & 0x3FFFFFFF)
#define SIZE_SET(varsize, len) ((varsize) = (((uint32_t)(len)) << 2))
#define IS_BIG_ENDIAN 0
#endif

/**
 * Parser check flags
 *
 *  @see lwgeom_from_wkb
 *  @see lwgeom_from_hexwkb
 *  @see lwgeom_parse_wkt
 */
#define LW_PARSER_CHECK_MINPOINTS  1
#define LW_PARSER_CHECK_ODD        2
#define LW_PARSER_CHECK_CLOSURE    4
#define LW_PARSER_CHECK_ZCLOSURE   8

#define LW_PARSER_CHECK_NONE   0
#define LW_PARSER_CHECK_ALL (LW_PARSER_CHECK_MINPOINTS | LW_PARSER_CHECK_ODD | LW_PARSER_CHECK_CLOSURE)

/**
* Return types for functions with status returns.
*/
#define LW_TRUE 1
#define LW_FALSE 0
#define LW_UNKNOWN 2
#define LW_FAILURE 0
#define LW_SUCCESS 1

/**
* Flags applied in EWKB to indicate Z/M dimensions and
* presence/absence of SRID and bounding boxes
*/
#define WKBZOFFSET  0x80000000
#define WKBMOFFSET  0x40000000
#define WKBSRIDFLAG 0x20000000
#define WKBBBOXFLAG 0x10000000


/**
* Well-Known Binary (WKB) Output Variant Types
*/

#define WKB_DOUBLE_SIZE 8 /* Internal use only */
#define WKB_INT_SIZE 4 /* Internal use only */
#define WKB_BYTE_SIZE 1 /* Internal use only */

/**
* Well-Known Binary (WKB) Geometry Types
*/
#define WKB_POINT_TYPE 1
#define WKB_LINESTRING_TYPE 2
#define WKB_POLYGON_TYPE 3
#define WKB_MULTIPOINT_TYPE 4
#define WKB_MULTILINESTRING_TYPE 5
#define WKB_MULTIPOLYGON_TYPE 6
#define WKB_GEOMETRYCOLLECTION_TYPE 7
#define WKB_CIRCULARSTRING_TYPE 8
#define WKB_COMPOUNDCURVE_TYPE 9
#define WKB_CURVEPOLYGON_TYPE 10
#define WKB_MULTICURVE_TYPE 11
#define WKB_MULTISURFACE_TYPE 12
#define WKB_CURVE_TYPE 13 /* from ISO draft, not sure is real */
#define WKB_SURFACE_TYPE 14 /* from ISO draft, not sure is real */
#define WKB_POLYHEDRALSURFACE_TYPE 15
#define WKB_TIN_TYPE 16
#define WKB_TRIANGLE_TYPE 17

/** Unknown SRID value */
#define SRID_UNKNOWN 0
#define SRID_IS_UNKNOWN(x) ((int)x<=0)


/**
* Used for passing the parse state between the parsing functions.
*/
typedef struct
{
    const uint8_t *wkb; /* Points to start of WKB */
    int32_t srid;    /* Current SRID we are handling */
    size_t wkb_size; /* Expected size of WKB */
    int8_t swap_bytes;  /* Do an endian flip? */
    int8_t check;       /* Simple validity checks on geometries */
    int8_t lwtype;      /* Current type we are handling */
    int8_t has_z;       /* Z? */
    int8_t has_m;       /* M? */
    int8_t has_srid;    /* SRID? */
    int8_t error;       /* An error was found (not enough bytes to read) */
    const uint8_t *pos; /* Current parse position */
} wkb_parse_state;

/**
* Check that we are not about to read off the end of the WKB
* array.
*/
static inline void wkb_parse_state_check(wkb_parse_state *s, size_t next)
{
    if( (s->pos + next) > (s->wkb + s->wkb_size) )
    {
        //lwerror("WKB structure does not match expected size!");
        s->error = LW_TRUE;
    }
}




/******************************************************************
* LWGEOM and GBOX both use LWFLAGS bit mask.
* Serializations (may) use different bit mask schemes.
*/
typedef uint16_t lwflags_t;

/******************************************************************
* GBOX structure.
* We include the flags (information about dimensionality),
* so we don't have to constantly pass them
* into functions that use the GBOX.
*/
typedef struct
{
    lwflags_t flags;
    double xmin;
    double xmax;
    double ymin;
    double ymax;
    double zmin;
    double zmax;
    double mmin;
    double mmax;
} GBOX;

/******************************************************************
* POINT2D, POINT3D, POINT3DM, POINT4D
*/
typedef struct
{
    double x, y;
}
        POINT2D;

typedef struct
{
    double x, y, z;
}
        POINT3DZ;

typedef struct
{
    double x, y, z;
}
        POINT3D;

typedef struct
{
    double x, y, m;
}
        POINT3DM;

typedef struct
{
    double x, y, z, m;
}
        POINT4D;

/******************************************************************
*  POINTARRAY
*  Point array abstracts a lot of the complexity of points and point lists.
*  It handles 2d/3d translation
*    (2d points converted to 3d will have z=0 or NaN)
*  DO NOT MIX 2D and 3D POINTS! EVERYTHING* is either one or the other
*/
typedef struct
{
    uint32_t npoints;   /* how many points we are currently storing */
    uint32_t maxpoints; /* how many points we have space for in serialized_pointlist */

    /* Use FLAGS_* macros to handle */
    lwflags_t flags;

    /* Array of POINT 2D, 3D or 4D, possibly misaligned. */
    uint8_t *serialized_pointlist;
}
        POINTARRAY;

/******************************************************************
* GSERIALIZED
*/

typedef struct
{
    uint32_t size; /* For PgSQL use only, use VAR* macros to manipulate. */
    uint8_t srid[3]; /* 24 bits of SRID */
    uint8_t gflags; /* HasZ, HasM, HasBBox, IsGeodetic */
    uint8_t data[1]; /* See gserialized.txt */
} GSERIALIZED;

/******************************************************************
* LWGEOM (any geometry type)
*
* Abstract type, note that 'type', 'bbox' and 'srid' are available in
* all geometry variants.
*/
typedef struct
{
    GBOX *bbox;
    void *data;
    int32_t srid;
    lwflags_t flags;
    uint8_t type;
    char pad[1]; /* Padding to 24 bytes (unused) */
}
        LWGEOM;

/* POINTYPE */
typedef struct
{
    GBOX *bbox;
    POINTARRAY *point;  /* hide 2d/3d (this will be an array of 1 point) */
    int32_t srid;
    lwflags_t flags;
    uint8_t type; /* POINTTYPE */
    char pad[1]; /* Padding to 24 bytes (unused) */
}
LWPOINT; /* "light-weight point" *

 /* POLYGONTYPE */
typedef struct
{
    GBOX *bbox;
    POINTARRAY **rings; /* list of rings (list of points) */
    int32_t srid;
    lwflags_t flags;
    uint8_t type; /* POLYGONTYPE */
    char pad[1]; /* Padding to 24 bytes (unused) */
    uint32_t nrings;   /* how many rings we are currently storing */
    uint32_t maxrings; /* how many rings we have space for in **rings */
}
        LWPOLY; /* "light-weight polygon" */

/* MULTIPOINTTYPE */
typedef struct
{
    GBOX *bbox;
    LWPOINT **geoms;
    int32_t srid;
    lwflags_t flags;
    uint8_t type; /* MULTYPOINTTYPE */
    char pad[1]; /* Padding to 24 bytes (unused) */
    uint32_t ngeoms;   /* how many geometries we are currently storing */
    uint32_t maxgeoms; /* how many geometries we have space for in **geoms */
}
        LWMPOINT;

/* MULTIPOLYGONTYPE */
typedef struct
{
    GBOX *bbox;
    LWPOLY **geoms;
    int32_t srid;
    lwflags_t flags;
    uint8_t type; /* MULTIPOLYGONTYPE */
    char pad[1]; /* Padding to 24 bytes (unused) */
    uint32_t ngeoms;   /* how many geometries we are currently storing */
    uint32_t maxgeoms; /* how many geometries we have space for in **geoms */
}
        LWMPOLY;

/* COLLECTIONTYPE */
typedef struct
{
    GBOX *bbox;
    LWGEOM **geoms;
    int32_t srid;
    lwflags_t flags;
    uint8_t type; /* COLLECTIONTYPE */
    char pad[1]; /* Padding to 24 bytes (unused) */
    uint32_t ngeoms;   /* how many geometries we are currently storing */
    uint32_t maxgeoms; /* how many geometries we have space for in **geoms */
}
        LWCOLLECTION;

/* CURVEPOLYTYPE */
typedef struct
{
    GBOX *bbox;
    LWGEOM **rings;
    int32_t srid;
    lwflags_t flags;
    uint8_t type; /* CURVEPOLYTYPE */
    char pad[1]; /* Padding to 24 bytes (unused) */
    uint32_t nrings;    /* how many rings we are currently storing */
    uint32_t maxrings;  /* how many rings we have space for in **rings */
}
        LWCURVEPOLY; /* "light-weight polygon" */

/*
 * Size of point represeneted in the POINTARRAY
 * 16 for 2d, 24 for 3d, 32 for 4d
 */
static inline size_t
ptarray_point_size(const POINTARRAY *pa)
{
    return sizeof(double) * FLAGS_NDIMS(pa->flags);
}

/****************************************************************
 * MEMORY MANAGEMENT
 ****************************************************************/

/*
* The *_free family of functions frees *all* memory associated
* with the pointer. When the recursion gets to the level of the
* POINTARRAY, the POINTARRAY is only freed if it is not flagged
* as "read only". LWGEOMs constructed on top of GSERIALIZED
* from PgSQL use read only point arrays.
*/

extern void ptarray_free(POINTARRAY *pa);
extern void lwpoint_free(LWPOINT *pt);
extern void lwpoly_free(LWPOLY *poly);
extern void lwmpoint_free(LWMPOINT *mpt);
extern void lwmpoly_free(LWMPOLY *mpoly);
extern void lwcollection_free(LWCOLLECTION *col);
extern void lwgeom_free(LWGEOM *geom);

/**
* Construct a new #POINTARRAY, <em>copying</em> in the data from ptlist
*/
extern POINTARRAY* ptarray_construct_copy_data(char hasz, char hasm, uint32_t npoints, const uint8_t *ptlist);

/**
* Construct an empty pointarray, allocating storage and setting
* the npoints, but not filling in any information. Should be used in conjunction
* with ptarray_set_point4d to fill in the information in the array.
*/
extern POINTARRAY* ptarray_construct(char hasz, char hasm, uint32_t npoints);

/**
* Create a new #POINTARRAY with no points. Allocate enough storage
* to hold maxpoints vertices before having to reallocate the storage
* area.
*/
extern POINTARRAY* ptarray_construct_empty(char hasz, char hasm, uint32_t maxpoints);

/*
* Empty geometry constructors.
*/
extern LWPOINT* lwpoint_construct_empty(int32_t srid, char hasz, char hasm);

/*
* Geometry constructors. These constructors to not copy the point arrays
* passed to them, they just take references, so do not free them out
* from underneath the geometries.
*/
extern LWPOINT* lwpoint_construct(int32_t srid, GBOX *bbox, POINTARRAY *point);

/**
* Return the type name string associated with a type number
* (e.g. Point, LineString, Polygon)
*/
extern const char *lwtype_name(uint8_t type);

/*
 * Get a pointer to Nth point of a POINTARRAY
 * You'll need to cast it to appropriate dimensioned point.
 * Note that if you cast to a higher dimensional point you'll
 * possibly corrupt the POINTARRAY.
 *
 * Casting to returned pointer to POINT2D* should be safe,
 * as gserialized format always keeps the POINTARRAY pointer
 * aligned to double boundary.
 *
 * WARNING: Don't cast this to a POINT!
 * it would not be reliable due to memory alignment constraints
 */
static inline uint8_t *
getPoint_internal(const POINTARRAY *pa, uint32_t n)
{
    size_t size;
    uint8_t *ptr;

#if PARANOIA_LEVEL > 0
    assert(pa);
	assert(n <= pa->npoints);
	assert(n <= pa->maxpoints);
#endif

    size = ptarray_point_size(pa);
    ptr = pa->serialized_pointlist + size * n;

    return ptr;
}
/**
 * Returns a POINT2D pointer into the POINTARRAY serialized_ptlist,
 * suitable for reading from. This is very high performance
 * and declared const because you aren't allowed to muck with the
 * values, only read them.
 */
static inline const POINT2D *
getPoint2d_cp(const POINTARRAY *pa, uint32_t n)
{
    return (const POINT2D *)getPoint_internal(pa, n);
}

/**
 * @param wkb_size length of WKB byte buffer
 * @param wkb WKB byte buffer
 * @param check parser check flags, see LW_PARSER_CHECK_* macros
 */
extern LWGEOM* lwgeom_from_wkb(const uint8_t *wkb, const size_t wkb_size, const char check);

#endif