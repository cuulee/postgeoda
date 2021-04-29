-------------------------------------
-- Author: Xun Li <lixun910@gmail.com>-
-- Date: 2021-4-21
-- Changes:
-- 2021-4-21 Expose knn_weights() as the major interface for queen weights creation
-- 2012-4-26 Add knn_weights(gid, geom, 4, power, is_arc, is_mile)
-- 2012-4-20 Add neighbor_match_test()
--------------------------------------

-- knn_weights(gid, geom, 4)
CREATE OR REPLACE FUNCTION knn_weights(anyelement, bytea, integer)
    RETURNS bytea
AS 'MODULE_PATHNAME', 'pg_knn_weights_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

-- knn_weights(gid, geom, 4, power, is_arc, is_mile)
CREATE OR REPLACE FUNCTION knn_weights(anyelement, bytea, integer, numeric, boolean, boolean)
    RETURNS bytea
AS 'MODULE_PATHNAME', 'pg_knn_weights_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

-- kernel_knn_weights(gid, geom, 4, 'gaussian')
CREATE OR REPLACE FUNCTION kernel_knn_weights(anyelement, bytea, integer, character varying)
    RETURNS bytea
AS 'MODULE_PATHNAME', 'pg_kernel_knn_weights_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

-- kernel_knn_weights(gid, geom, 4, 'gaussian', power, is_inverse, is_arc, is_mile,
-- adaptive_bandwidth, use_kernel_diagonals)
CREATE OR REPLACE FUNCTION kernel_knn_weights(anyelement, bytea, integer, character varying,
    numeric, boolean, boolean, boolean, boolean, boolean)
    RETURNS bytea
AS 'MODULE_PATHNAME', 'pg_kernel_knn_weights_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;


-- neighbor_match_test(ARRAY[ep_pov, ep_unem], geom, 4)
-- pg_neighbor_match_test_window
CREATE OR REPLACE FUNCTION neighbor_match_test(bytea, anyelement, integer)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_neighbor_match_test_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

-- neighbor_match_test(ARRAY[ep_pov, ep_unem], 4, power, is_inverse, is_arc, is_mile, "standardize", "euclidean")
CREATE OR REPLACE FUNCTION neighbor_match_test(bytea, anyelement, integer, numeric, boolean, boolean, boolean,
    character varying, character varying)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_neighbor_match_test_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--- the following functions be depreciated
---
-------------------------------------------------------------------
-- knn_weights('ogc_fid', 'wkb_geometry', 4, 'knn4', 'nat')
-- DEPENDENCIES
-- geoda_weights_getfids()
-- geoda_weights_toset()
-- geoda_weights_knn()
-------------------------------------------------------------------
CREATE OR REPLACE FUNCTION knn_weights1(
    fid VARCHAR, geom VARCHAR, k INTEGER, colName VARCHAR, tableName VARCHAR
) RETURNS
    TABLE (w bytea)
AS $$
BEGIN
    EXECUTE 'ALTER TABLE '|| tableName ||' ADD COLUMN IF NOT EXISTS '|| colName ||' bytea;';
    EXECUTE '
        WITH tmp_w AS (
            SELECT
                geoda_weights_knn('|| fid ||', '|| geom ||', '|| k ||') AS w
            FROM '|| tableName ||'
        )
        UPDATE '|| tableName ||' ta SET '|| colName ||' = tb.w
        FROM (
                 SELECT
                     geoda_weights_getfids(w) AS fid,
                     geoda_weights_toset(w) AS w
                 FROM tmp_w
             ) AS tb
        WHERE ta.'||fid||' = tb.fid';
    RETURN QUERY EXECUTE
        'SELECT '|| geom ||' w FROM ' || tableName ||' ORDER BY '|| fid;
END;
$$ LANGUAGE 'plpgsql';


--------------------------------------
-- geoda_weights_knn(ogc_fid, wkb_geometry, k=4)
-- AGGREGATE function
-- DEPENDENCIES
-- sfunc: bytea_knn_to_geom_transfn()
-- finalfunc: geom_knn_weights_bin_finalfn()
--------------------------------------
CREATE OR REPLACE FUNCTION
    geom_knn_weights_bin_finalfn(internal)
    RETURNS bytea
AS 'MODULE_PATHNAME', 'geom_knn_weights_bin_finalfn'
    LANGUAGE c PARALLEL SAFE;

CREATE OR REPLACE FUNCTION
    bytea_knn_geom_transfn(internal, integer, bytea, integer)
    RETURNS internal
AS 'MODULE_PATHNAME', 'bytea_knn_geom_transfn'
    LANGUAGE c PARALLEL SAFE;

CREATE AGGREGATE geoda_weights_knn(integer, bytea, integer) (
    sfunc = bytea_knn_geom_transfn,
    stype = internal,
    finalfunc = geom_knn_weights_bin_finalfn
    );

--------------------------------------
-- geoda_weights_knn(ogc_fid, wkb_geometry, 4, 1, FALSE, FALSE)
--------------------------------------
CREATE OR REPLACE FUNCTION
    bytea_knn_geom_transfn(internal, integer, bytea, integer, integer, boolean, boolean)
    RETURNS internal
AS 'MODULE_PATHNAME', 'bytea_knn_geom_transfn'
    LANGUAGE c PARALLEL SAFE;

CREATE AGGREGATE geoda_weights_knn(integer, bytea, integer, integer, boolean, boolean) (
    sfunc = bytea_knn_geom_transfn,
    stype = internal,
    finalfunc = geom_knn_weights_bin_finalfn
    );