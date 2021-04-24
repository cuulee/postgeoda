-------------------------------------
-- Author: Xun Li <lixun910@gmail.com>
-- Date: 2021-4-23
-- Changes:
-- 2021-4-23 Add distance_weights() as the major interface for queen weights creation
--------------------------------------

--------------------------------------
-- MAIN INTERFACE min_distthreshold(wkb_geometry)
--------------------------------------
CREATE OR REPLACE FUNCTION bytea_to_geom_dist_transfn(
    internal, integer, bytea
)
    RETURNS internal
AS 'MODULE_PATHNAME', 'bytea_to_geom_dist_transfn'
    LANGUAGE c PARALLEL SAFE;

CREATE OR REPLACE FUNCTION bytea_to_geom_dist_transfn(
    internal, integer, bytea, boolean, boolean
)
    RETURNS internal
AS 'MODULE_PATHNAME', 'bytea_to_geom_dist_transfn'
    LANGUAGE c PARALLEL SAFE;

CREATE OR REPLACE FUNCTION
    geom_to_dist_threshold_finalfn(internal)
    RETURNS FLOAT4
AS 'MODULE_PATHNAME', 'geom_to_dist_threshold_finalfn'
    LANGUAGE c PARALLEL SAFE;

CREATE AGGREGATE min_distthreshold(integer, bytea) (
    sfunc = bytea_to_geom_dist_transfn,
    stype = internal,
    finalfunc = geom_to_dist_threshold_finalfn
    );

CREATE AGGREGATE min_distthreshold(integer, bytea, boolean, boolean) (
    sfunc = bytea_to_geom_dist_transfn,
    stype = internal,
    finalfunc = geom_to_dist_threshold_finalfn
    );

--------------------------------------
-- MAIN INTERFACE distance_weights(fid, wkb_geometry, 103.0)
--------------------------------------
CREATE OR REPLACE FUNCTION distance_weights(anyelement, bytea, float4)
    RETURNS bytea
AS 'MODULE_PATHNAME', 'pg_distance_weights_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- MAIN INTERFACE distance_weights(fid, wkb_geometry, 103.0, 1, FALSE, FALSE, TRUE)
--------------------------------------
CREATE OR REPLACE FUNCTION distance_weights(
    fid anyelement,
    geom bytea,
    dist_thres float4,
    power float4,
    is_inverse boolean,
    is_arc boolean,
    is_mile boolean
) RETURNS bytea
AS 'MODULE_PATHNAME', 'pg_distance_weights_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;


