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


