-------------------------------------
-- Author: Xun Li <lixun910@gmail.com>-
-- Date: 2021-1-27
-- Changes:
-- 2021-1-27 Reorganize Weights SQLs
-- 2021-4-10 Expose queen_weights() as the major interface for queen weights creation
-- 2021-4-23 Add Window SQL functions for queen_weights and rook_weights
--------------------------------------

--------------------------------------
-- MAIN INTERFACE queen_weights(fid, wkb_geometry)
--------------------------------------
CREATE OR REPLACE FUNCTION queen_weights(integer, bytea)
    RETURNS bytea
AS 'MODULE_PATHNAME', 'pg_queen_weights_window'
    LANGUAGE 'c' STRICT WINDOW;

--------------------------------------
-- MAIN INTERFACE queen_weights(fid, wkb_geometry, 2, TRUE, 0.0)
--------------------------------------
CREATE OR REPLACE FUNCTION queen_weights(integer, bytea, integer, boolean, float4)
    RETURNS bytea
AS 'MODULE_PATHNAME', 'pg_queen_weights_window'
    LANGUAGE 'c' STRICT WINDOW;

--------------------------------------
-- MAIN INTERFACE rook_weights(fid, wkb_geometry)
--------------------------------------
CREATE OR REPLACE FUNCTION rook_weights(integer, bytea)
    RETURNS bytea
AS 'MODULE_PATHNAME', 'pg_rook_weights_window'
    LANGUAGE 'c' STRICT WINDOW;

--------------------------------------
-- MAIN INTERFACE rook_weights(fid, wkb_geometry, 2, TRUE, 0.0)
--------------------------------------
CREATE OR REPLACE FUNCTION rook_weights(integer, bytea, integer, boolean, float4)
    RETURNS bytea
AS 'MODULE_PATHNAME', 'pg_rook_weights_window'
    LANGUAGE 'c' STRICT WINDOW;

-------------------------------------------------------------------
-- queen_weights/rook_weights('ogc_fid', 'wkb_geometry', 'queen_w', 'nat')
-- DEPENDENCIES
-- geoda_weights_getfids()
-- geoda_weights_toset()
-- geoda_weights_cont()
-------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen_weights(
    fid VARCHAR, geom VARCHAR, colName VARCHAR, tableName VARCHAR
) RETURNS
    TABLE (w bytea)
AS $$
BEGIN
    EXECUTE 'ALTER TABLE '|| tableName ||' ADD COLUMN IF NOT EXISTS '|| colName ||' bytea;';
    EXECUTE '
        WITH tmp_w AS (
            SELECT
                geoda_weights_cont('|| fid ||', '|| geom ||', TRUE) AS w
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

CREATE OR REPLACE FUNCTION rook_weights(
    fid VARCHAR, geom VARCHAR, colName VARCHAR, tableName VARCHAR
) RETURNS
    TABLE (w bytea)
AS $$
BEGIN
    EXECUTE 'ALTER TABLE '|| tableName ||' ADD COLUMN IF NOT EXISTS '|| colName ||' bytea;';
    EXECUTE '
        WITH tmp_w AS (
            SELECT
                geoda_weights_cont('|| fid ||', '|| geom ||', FALSE) AS w
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
    RETURN QUERY EXECUTE 'SELECT '|| geom ||' w FROM ' || tableName ||' ORDER BY '|| fid;
END;
$$ LANGUAGE 'plpgsql';


-------------------------------------------------------------------
-- queen_weights/rook_weights('ogc_fid', 'wkb_geometry', 2, true, 0, 'queen_w', 'nat')
-- DEPENDENCIES
-- geoda_weights_getfids()
-- geoda_weights_toset()
-- geoda_weights_cont()
-------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen_weights(
    fid VARCHAR, geom VARCHAR, ordCont INTEGER, incLoOrd BOOLEAN, precThreshold FLOAT4, colName VARCHAR, tableName VARCHAR
) RETURNS
    TABLE (w bytea)
AS $$
BEGIN
    EXECUTE 'ALTER TABLE '|| tableName ||' ADD COLUMN IF NOT EXISTS '|| colName ||' bytea;';
    EXECUTE '
        WITH tmp_w AS (
            SELECT
                geoda_weights_cont('|| fid ||', '|| geom ||', TRUE, '|| ordCont ||', '|| incLoOrd || ', '|| precThreshold ||') AS w
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

CREATE OR REPLACE FUNCTION rook_weights(
    fid VARCHAR, geom VARCHAR, ordCont INTEGER, incLoOrd BOOLEAN, precThreshold FLOAT4, colName VARCHAR, tableName VARCHAR
) RETURNS
    TABLE (w bytea)
AS $$
BEGIN
    EXECUTE 'ALTER TABLE '|| tableName ||' ADD COLUMN IF NOT EXISTS '|| colName ||' bytea;';
    EXECUTE '
        WITH tmp_w AS (
            SELECT
                geoda_weights_cont('|| fid ||', '|| geom ||', FALSE, '|| ordCont ||', '|| incLoOrd || ', '|| precThreshold ||') AS w
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
    RETURN QUERY EXECUTE 'SELECT '|| geom ||' w FROM ' || tableName ||' ORDER BY '|| fid;
END;
$$ LANGUAGE 'plpgsql';

--------------------------------------
-- geoda_weights_cont(ogc_fid, wkb_geometry, TRUE)
-- geoda_weights_cont(ogc_fid, wkb_geometry, TRUE, 2, TRUE, 0.0)
-- AGGREGATE
-- DEPENDENCIES
-- bytea_to_geom_transfn()
-- geom_to_contweights_finalfn()
--------------------------------------
CREATE OR REPLACE FUNCTION bytea_to_geom_transfn(
    internal,
    integer default null,
    bytea default null,
    boolean default true,
    integer default 1,
    boolean default false,
    float4 default 0
)
    RETURNS internal
AS 'MODULE_PATHNAME', 'bytea_to_geom_transfn'
    LANGUAGE c PARALLEL SAFE;

CREATE OR REPLACE FUNCTION bytea_to_geom_transfn(
    internal,
    integer,
    bytea,
    boolean
)
    RETURNS internal
AS 'MODULE_PATHNAME', 'bytea_to_geom_transfn'
    LANGUAGE c PARALLEL SAFE;

CREATE OR REPLACE FUNCTION
    geom_to_contweights_finalfn(internal)
    RETURNS bytea
AS 'MODULE_PATHNAME', 'geom_to_contweights_finalfn'
    LANGUAGE c PARALLEL SAFE;

CREATE AGGREGATE geoda_weights_cont(integer, bytea, boolean, integer, boolean, float4) (
    sfunc = bytea_to_geom_transfn,
    stype = internal,
    finalfunc = geom_to_contweights_finalfn
    );

CREATE AGGREGATE geoda_weights_cont(fid integer, geom bytea, is_queen boolean)(
    sfunc = bytea_to_geom_transfn,
    stype = internal,
    finalfunc = geom_to_contweights_finalfn
    );

--------------------------------------
-- geoda_weights_toset(bytea)
-- bytea of complete weights
--------------------------------------
CREATE OR REPLACE FUNCTION geoda_weights_toset(bytea)
    RETURNS SETOF bytea
AS 'MODULE_PATHNAME', 'weights_bytea_to_set'
    LANGUAGE C PARALLEL SAFE;

--------------------------------------
-- geoda_weights_getfid(bytea)
-- bytea of complete weights
--------------------------------------
CREATE OR REPLACE FUNCTION geoda_weights_getfids(bytea)
    RETURNS SETOF integer
AS 'MODULE_PATHNAME', 'weights_bytea_getfids'
    LANGUAGE C PARALLEL SAFE;

--------------------------------------
-- geoda_weights_tojson(bytea)
-- bytea of complete weights
--------------------------------------
CREATE OR REPLACE FUNCTION geoda_weights_tojson(bytea)
    RETURNS text
AS 'MODULE_PATHNAME', 'weights_bytea_tojson'
    LANGUAGE c PARALLEL SAFE;


--------------------------------------
-- weights_astext(bytea)
-- bytea of weights of ONE observation
--------------------------------------
CREATE OR REPLACE FUNCTION weights_astext(bytea)
    RETURNS text
AS 'MODULE_PATHNAME', 'weights_to_text'
    LANGUAGE c PARALLEL SAFE
    COST 100;

--------------------------------------
-- DEPRECATED: geoda_weights_at()
--------------------------------------
CREATE OR REPLACE FUNCTION geoda_weights_at(integer, bytea)
    RETURNS bytea
AS 'MODULE_PATHNAME', 'PGWeight_at'
    LANGUAGE C PARALLEL SAFE
               COST 100;

--------------------------------------
-- DEPRECATED: geoda_queenweights_set()
--------------------------------------
CREATE OR REPLACE FUNCTION geoda_queenweights_set(bytea)
    RETURNS SETOF bytea
AS 'MODULE_PATHNAME', 'PGWeight_to_set'
    LANGUAGE C STRICT PARALLEL SAFE;


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

-- knn_weights_sub(gid, geom, 4, 0, 10000)
CREATE OR REPLACE FUNCTION knn_weights_sub(anyelement, bytea, integer, integer, integer)
    RETURNS bytea
AS 'MODULE_PATHNAME', 'pg_knn_weights_sub_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

-- knn_weights(gid, geom, 4, power, is_inverse, is_arc, is_mile)
CREATE OR REPLACE FUNCTION knn_weights(anyelement, bytea, integer, numeric, boolean, boolean)
    RETURNS bytea
AS 'MODULE_PATHNAME', 'pg_knn_weights_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

-- kernel_knn_weights(gid, geom, 4, 'gaussian')
CREATE OR REPLACE FUNCTION kernel_knn_weights(anyelement, bytea, integer, character varying)
    RETURNS bytea
AS 'MODULE_PATHNAME', 'pg_kernel_knn_weights_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

-- kernel_knn_weights(gid, geom, 4, 'gaussian', adaptive_bandwidth)
CREATE OR REPLACE FUNCTION kernel_knn_weights(anyelement, bytea, integer, character varying, boolean)
    RETURNS bytea
AS 'MODULE_PATHNAME', 'pg_kernel_knn_weights_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

-- kernel_knn_weights(gid, geom, 4, 'gaussian', adaptive_bandwidth, use_kernel_diagonals,
-- power, is_inverse, is_arc, is_mile)
CREATE OR REPLACE FUNCTION kernel_knn_weights(anyelement, bytea, integer, character varying, boolean,
                                              boolean, numeric, boolean, boolean, boolean)
    RETURNS bytea
AS 'MODULE_PATHNAME', 'pg_kernel_knn_weights_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;


-- neighbor_match_test(ARRAY[ep_pov, ep_unem], geom, 4)
-- pg_neighbor_match_test_window
CREATE OR REPLACE FUNCTION neighbor_match_test(bytea, anyarray, integer)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_neighbor_match_test_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

CREATE OR REPLACE FUNCTION neighbor_match_test(bytea, anyarray, integer, character varying, character varying)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_neighbor_match_test_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

-- neighbor_match_test(ARRAY[ep_pov, ep_unem], 4, power, is_inverse, is_arc, is_mile, "standardize", "euclidean")
CREATE OR REPLACE FUNCTION neighbor_match_test(bytea, anyarray, integer,
                                               character varying, character varying, float, boolean, boolean, boolean
    )
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
-------------------------------------
-- Author: Xun Li <lixun910@gmail.com>
-- Date: 2021-4-23
-- Changes:
-- 2021-4-23 Add distance_weights() as the major interface for queen weights creation
-- 2021-4-26 Add min_distthreshold()
-- 2021-4-27 Add kernel_weights()
--------------------------------------

--------------------------------------
-- MAIN INTERFACE min_distthreshold(gid, wkb_geometry)
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

--------------------------------------
-- MAIN INTERFACE kernel_weights(fid, wkb_geometry, 103.0, 'gaussian')
--------------------------------------
CREATE OR REPLACE FUNCTION kernel_weights(anyelement, bytea, float8, character varying)
    RETURNS bytea
AS 'MODULE_PATHNAME', 'pg_kernel_weights_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

-- kernel_weights(gid, geom, 103.0, 'gaussian', use_kernel_diagonals, power, is_inverse, is_arc, is_mile,
-- use_kernel_diagonals)
CREATE OR REPLACE FUNCTION kernel_weights(anyelement, bytea, float8, character varying, boolean,
                                          float8, boolean, boolean, boolean)
    RETURNS bytea
AS 'MODULE_PATHNAME', 'pg_kernel_weights_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;
-------------------------------------
-- Author: Xun Li <lixun910@gmail.com>-
-- Date: 2021-1-27
-- Changes:
-- 2021-1-27 add geoda_localjoincount_b()
-- 2021-1-29 add bivariate geoda_blocaljoincount_b(), multivariate geoda_mlocaljoincount_b()
-- 2021-4-10 add geoda_quantilelisa
-- 2021-4-27 rename and reorganize lisa functions
-- add local_moran() with array output; add local_moran() with arguments: permutations, method, significance_cutoff,
-- cpu_threads and seed
--------------------------------------

--------------------------------------
-- local_moran(crm_prs, bytea)
-- MAIN INTERFACE for local moran function
--------------------------------------
CREATE OR REPLACE FUNCTION local_moran(anyelement, bytea)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_moran_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

CREATE OR REPLACE FUNCTION local_moran(anyelement, bytea, integer, character varying, float8, integer, integer)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_moran_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- local_moran_fast(crm_prs, bytea)
-- select "Crm_prs", wkb_geometry, Array(select "Crm_prs" from guerry) as abc FROM guerry;
--------------------------------------
CREATE OR REPLACE FUNCTION local_moran_fast(anyelement, bytea, anyarray)
    RETURNS point
AS 'MODULE_PATHNAME', 'pg_local_moran_fast'
    LANGUAGE 'c' PARALLEL SAFE COST 1000;
--------------------------------------
-- local_moran_b(crm_prs, bytea)
-- the weights should be passed as a whole in BYTEA format
-- (This function will be depcreciated)
--------------------------------------
CREATE OR REPLACE FUNCTION local_moran_b(integer, anyelement, bytea)
    RETURNS point
AS 'MODULE_PATHNAME', 'pg_local_moran_window_bytea'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;
-------------------------------------
-- Author: Xun Li <lixun910@gmail.com>-
-- Date: 2021-4-27
-- Changes:
-- 2021-4-27 add local_g(), local_gstar()
-- 2021-4-28 remove 'ogc_fid' from local_g/gstar(); add full versions of SQL queries
--------------------------------------

--------------------------------------
-- local_g(crm_prs, bytea)
--------------------------------------
CREATE OR REPLACE FUNCTION local_g(anyelement, bytea)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_g_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

CREATE OR REPLACE FUNCTION local_g(anyelement, bytea, integer, character varying, float8, integer, integer)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_g_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- local_gstar(crm_prs, bytea)
--------------------------------------
CREATE OR REPLACE FUNCTION local_gstar(anyelement, bytea)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_gstar_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

CREATE OR REPLACE FUNCTION local_gstar(anyelement, bytea, integer, character varying, float8, integer, integer)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_gstar_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;
-------------------------------------
-- Author: Xun Li <lixun910@gmail.com>-
-- Date: 2021-5-6
-- Changes:
-- 2021-5-6 add local_geary, local_multigeary
--------------------------------------

--------------------------------------
-- local_geary(crm_prs, queen_w)
--------------------------------------
CREATE OR REPLACE FUNCTION local_geary(anyelement, bytea)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_geary_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- local_geary(crm_prs, queen_w, 999, 'complete', 0.01, 6, 123456789)
--------------------------------------
CREATE OR REPLACE FUNCTION local_geary(
    anyelement, bytea, integer, character varying, float8, integer, integer
)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_geary_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- local_multigeary(ARRAY[ep_pov, ep_pci], queen_w)
--------------------------------------
CREATE OR REPLACE FUNCTION local_multigeary(anyarray, bytea)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_multigeary_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

CREATE OR REPLACE FUNCTION local_multigeary(
    anyarray, bytea, integer, character varying, float8, integer, integer
)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_multigeary_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;
-------------------------------------
-- Author: Xun Li <lixun910@gmail.com>-
-- Date: 2021-4-27
-- Changes:
-- 2021-4-27 add local_joincount(), add local_bijoincount(), local_multijoincount()
-- 2021-4-28 add full version of the 3 query functions
--------------------------------------

--------------------------------------
-- local_joincount(crm_prs, bytea)
--------------------------------------
CREATE OR REPLACE FUNCTION local_joincount(anyelement, bytea)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_joincount_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

CREATE OR REPLACE FUNCTION local_joincount(anyelement, bytea, integer, character varying, float8, integer, integer)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_joincount_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- local_bijoincount(crm_prs, litercy, bytea)
--------------------------------------
CREATE OR REPLACE FUNCTION local_bijoincount(anyelement, anyelement, bytea)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_bijoincount_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

CREATE OR REPLACE FUNCTION local_bijoincount(anyelement, anyelement, bytea, integer, character varying, float8, integer, integer)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_bijoincount_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- local_multijoincount(ARRAY["Crm_prs", "Crm_prp"], bytea)
--------------------------------------
CREATE OR REPLACE FUNCTION local_multijoincount(anyarray, bytea)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_multijoincount_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

CREATE OR REPLACE FUNCTION local_multijoincount(anyarray, bytea, integer, character varying, float8, integer, integer)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_multijoincount_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;
-------------------------------------
-- Author: Xun Li <lixun910@gmail.com>-
-- Date: 2021-4-27
-- Changes:
-- 2021-4-27 add local_quantilelisa(), local_multiquantilelisa()
-- 2021-4-28 add full version of  local_quantilelisa() and local_multiquantilelisa()
--------------------------------------

--------------------------------------
-- quantile_lisa(5, 1, crm_prs, bytea)
--------------------------------------
CREATE OR REPLACE FUNCTION quantile_lisa(integer, integer, anyelement, bytea)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_quantilelisa_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

CREATE OR REPLACE FUNCTION quantile_lisa(
    integer, integer, anyelement, bytea, integer, character varying, float8, integer, integer
)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_quantilelisa_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- multiquantile_lisa(ARRAY[5,5], ARRAY[1, 1], ARRAY[ep_pov, ep_pci], queen_w)
--------------------------------------
CREATE OR REPLACE FUNCTION multiquantile_lisa(anyarray, anyarray, anyarray, bytea)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_multiquantilelisa_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

CREATE OR REPLACE FUNCTION multiquantile_lisa(
    anyarray, anyarray, anyarray, bytea, integer, character varying, float8, integer, integer
)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_multiquantilelisa_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;
-------------------------------------
-- Author: Xun Li <lixun910@gmail.com>-
-- Date: 2021-4-29
-- Changes:
--------------------------------------

-- variable_transfn()
-- common internal function used by all breaks functions
CREATE OR REPLACE FUNCTION variable_transfn(internal, anyelement)
    RETURNS internal
AS 'MODULE_PATHNAME', 'variable_transfn'
    LANGUAGE c PARALLEL SAFE;

CREATE OR REPLACE FUNCTION variable_transfn(internal, anyelement, integer)
    RETURNS internal
AS 'MODULE_PATHNAME', 'variable_transfn'
    LANGUAGE c PARALLEL SAFE;

--------------------------------------
-- hinge15_breaks(crm_prs)
-- AGGREGATE
--------------------------------------

CREATE OR REPLACE FUNCTION hinge15_finalfn(internal)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'hinge15_finalfn'
    LANGUAGE c PARALLEL SAFE;

CREATE AGGREGATE hinge15_breaks(anyelement) (
    sfunc = variable_transfn,
    stype = internal,
    finalfunc = hinge15_finalfn
    );

--------------------------------------
-- hinge30_breaks(crm_prs)
-- AGGREGATE
--------------------------------------

CREATE OR REPLACE FUNCTION hinge30_finalfn(internal)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'hinge30_finalfn'
    LANGUAGE c PARALLEL SAFE;

CREATE AGGREGATE hinge30_breaks(anyelement) (
    sfunc = variable_transfn,
    stype = internal,
    finalfunc = hinge30_finalfn
    );

--------------------------------------
-- percentile_breaks(crm_prs)
-- AGGREGATE
--------------------------------------

CREATE OR REPLACE FUNCTION percentile_finalfn(internal)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'percentile_finalfn'
    LANGUAGE c PARALLEL SAFE;

CREATE AGGREGATE percentile_breaks(anyelement) (
    sfunc = variable_transfn,
    stype = internal,
    finalfunc = percentile_finalfn
    );

--------------------------------------
-- stddev_breaks(crm_prs)
-- AGGREGATE
--------------------------------------

CREATE OR REPLACE FUNCTION stddev_finalfn(internal)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'stddev_finalfn'
    LANGUAGE c PARALLEL SAFE;

CREATE AGGREGATE stddev_breaks(anyelement) (
    sfunc = variable_transfn,
    stype = internal,
    finalfunc = stddev_finalfn
    );

--------------------------------------
-- quantile_breaks(k, crm_prs)
-- AGGREGATE
--------------------------------------

CREATE OR REPLACE FUNCTION quantile_finalfn(internal)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'quantile_finalfn'
    LANGUAGE c PARALLEL SAFE;

CREATE AGGREGATE quantile_breaks(anyelement, integer) (
    sfunc = variable_transfn,
    stype = internal,
    finalfunc = quantile_finalfn
    );

--------------------------------------
-- natural_breaks(k, crm_prs)
-- AGGREGATE
--------------------------------------

CREATE OR REPLACE FUNCTION naturalbreaks_finalfn(internal)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'naturalbreaks_finalfn'
    LANGUAGE c PARALLEL SAFE;

CREATE AGGREGATE natural_breaks(anyelement, integer) (
    sfunc = variable_transfn,
    stype = internal,
    finalfunc = naturalbreaks_finalfn
    );
-------------------------------------
-- Author: Xun Li <lixun910@gmail.com>-
-- Date: 2021-4-30
-- Changes:
-- 2021-4-30 add skater()
--------------------------------------

--------------------------------------
-- skater(5, ARRAY["Crm_prs", "Crm_prp"], bytea)
--------------------------------------
CREATE OR REPLACE FUNCTION skater(integer, anyarray, bytea)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_skater1_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- skater(5, ARRAY["Crm_prs", "Crm_prp"], bytea, min_region_size=10)
--------------------------------------
CREATE OR REPLACE FUNCTION skater(integer, anyarray, bytea, integer)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_skater1_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- skater(5, ARRAY["Crm_prs", "Crm_prp"], bytea, min_region_size=10, scale_method, distance_type, seed, cpu_threads)
--------------------------------------
CREATE OR REPLACE FUNCTION skater(
    integer, anyarray, bytea, integer, character varying, character varying, integer, integer
)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_skater1_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- skater(5, ARRAY["Crm_prs", "Crm_prp"], bytea, min_bound=Pop1831, min_bound_val=3236.67)
--------------------------------------
CREATE OR REPLACE FUNCTION skater(integer, anyarray, bytea, anyelement, float)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_skater2_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

CREATE OR REPLACE FUNCTION skater(integer, anyarray, bytea, bigint, float)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_skater2_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- skater(5, ARRAY["Crm_prs", "Crm_prp"], bytea, min_bound=Pop1831, min_bound_val=3236.67, scale_method, distance_type, seed, cpu_threads)
--------------------------------------
CREATE OR REPLACE FUNCTION skater(
    integer, anyarray, bytea, anyelement, float, character varying, character varying, integer, integer
)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_skater2_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

CREATE OR REPLACE FUNCTION skater(
    integer, anyarray, bytea, bigint, float, character varying, character varying, integer, integer
)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_skater2_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;


--------------------------------------
-- skater(ARRAY["Crm_prs", "Crm_prp"], bytea, min_bound=Pop1831, min_bound_val=3236.67)
--------------------------------------
CREATE OR REPLACE FUNCTION skater(anyarray, bytea, anyelement, float)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_skater3_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

CREATE OR REPLACE FUNCTION skater(anyarray, bytea, bigint, float)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_skater3_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;
-------------------------------------
-- Author: Xun Li <lixun910@gmail.com>-
-- Date: 2021-4-30
-- Changes:
-- 2021-5-1 add redcap()
--------------------------------------

--------------------------------------
-- redcap(5, ARRAY["Crm_prs", "Crm_prp"], bytea, "firstorder-singlelinkage")
--------------------------------------
CREATE OR REPLACE FUNCTION redcap(integer, anyarray, bytea, character varying)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_redcap1_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- redcap(5, ARRAY["Crm_prs", "Crm_prp"], bytea, "firstorder-singlelinkage", min_region_size=10)
--------------------------------------
CREATE OR REPLACE FUNCTION redcap(integer, anyarray, bytea, character varying, integer)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_redcap1_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- redcap(5, ARRAY["Crm_prs", "Crm_prp"], bytea, "firstorder-singlelinkage", min_region_size=10, scale_method, distance_type, seed, cpu_threads)
--------------------------------------
CREATE OR REPLACE FUNCTION redcap(
    integer, anyarray, bytea, character varying, integer, character varying, character varying, integer, integer
)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_redcap1_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- redcap(5, ARRAY["Crm_prs", "Crm_prp"], bytea, "firstorder-singlelinkage", min_bound=Pop1831, min_bound_val=3236.67)
--------------------------------------
CREATE OR REPLACE FUNCTION redcap(integer, anyarray, bytea, character varying, anyelement, float)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_redcap2_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

CREATE OR REPLACE FUNCTION redcap(integer, anyarray, bytea, character varying, bigint, float)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_redcap2_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- redcap(5, ARRAY["Crm_prs", "Crm_prp"], bytea, "firstorder-singlelinkage", min_bound=Pop1831, min_bound_val=3236.67, scale_method, distance_type, seed, cpu_threads)
--------------------------------------
CREATE OR REPLACE FUNCTION redcap(
    integer, anyarray, bytea, character varying, anyelement, float, character varying, character varying, integer, integer
)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_redcap2_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

CREATE OR REPLACE FUNCTION redcap(
    integer, anyarray, bytea, character varying, bigint, float, character varying, character varying, integer, integer
)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_redcap2_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;


--------------------------------------
-- redcap(ARRAY["Crm_prs", "Crm_prp"], bytea, "firstorder-singlelinkage", min_bound=Pop1831, min_bound_val=3236.67)
--------------------------------------
CREATE OR REPLACE FUNCTION redcap(anyarray, bytea, character varying, anyelement, float)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_redcap3_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

CREATE OR REPLACE FUNCTION redcap(anyarray, bytea, character varying, bigint, float)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_redcap3_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;
-------------------------------------
-- Author: Xun Li <lixun910@gmail.com>-
-- Date: 2021-5-7
--------------------------------------

--------------------------------------
-- excess_risk(event_variable, base_variable)
--------------------------------------
CREATE OR REPLACE FUNCTION excess_risk(anyelement, anyelement)
    RETURNS float8
AS 'MODULE_PATHNAME', 'pg_excess_risk'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- eb_rate(event_variable, base_variable)
--------------------------------------
CREATE OR REPLACE FUNCTION eb_rate(anyelement, anyelement)
    RETURNS float8
AS 'MODULE_PATHNAME', 'pg_eb_rate'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;


--------------------------------------
-- spatial_lag(variable, queen_w)
--------------------------------------
CREATE OR REPLACE FUNCTION spatial_lag(anyelement, bytea)
    RETURNS float8
AS 'MODULE_PATHNAME', 'pg_spatial_lag'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;


-- spatial_lag(variable, queen_w, is_binary, row_standardize, include_diagonal)
CREATE OR REPLACE FUNCTION spatial_lag(anyelement, bytea, boolean, boolean, boolean)
    RETURNS float8
AS 'MODULE_PATHNAME', 'pg_spatial_lag'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;
--------------------------------------
-- spatial_rate(variable, queen_w)
--------------------------------------
CREATE OR REPLACE FUNCTION spatial_rate(anyelement, anyelement, bytea)
    RETURNS float8
AS 'MODULE_PATHNAME', 'pg_spatial_rate'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;


--------------------------------------
-- spatial_eb(event_variable, base_variable, queen_w)
--------------------------------------
CREATE OR REPLACE FUNCTION spatial_eb(anyelement, anyelement, bytea)
    RETURNS float8
AS 'MODULE_PATHNAME', 'pg_spatial_eb'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;
