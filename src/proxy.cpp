#include "geoms.h"
#include "utils.h"

#include "proxy.h"

PGWeight* create_queen_weights(Oid geomOid, List *lwgeoms)
{
    ListCell *l;
    size_t nelems = 0;
    int16 elmlen;
    bool elmbyval;
    char elmalign;
    size_t i = 0;

    /* Retrieve geometry type metadata */
    get_typlenbyvalalign(geomOid, &elmlen, &elmbyval, &elmalign);
    nelems = list_length(lwgeoms);

    foreach (l, lwgeoms)
    {
        LWGEOM *lwgeom = (LWGEOM * )(lfirst(l));

        if (lwgeom->type == POINTTYPE) {
            LWPOINT *pt = lwgeom_as_lwpoint(lwgeom);
        } else if (lwgeom->type == MULTIPOINTTYPE) {
            LWMPOINT *mpt = lwgeom_as_lwmpoint(lwgeom);
        } else if (lwgeom->type == POLYGONTYPE) {
            LWPOLY *poly = lwgeom_as_lwpoly(lwgeom);
        } else if (lwgeom->type == MULTIPOLYGONTYPE) {
            LWMPOLY* mpoly = lwgeom_as_lwmpoly(lwgeom);
        } else {
            LWDEBUGF(4, "Unknown WKB type %s\n", lwtype_name(lwgeom->type));
        }

        /* Free both the original and geometries */
        //lwgeom_free(lwgeom);
    }

    return NULL;
}
