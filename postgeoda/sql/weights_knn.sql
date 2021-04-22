-------------------------------------
-- Author: Xun Li <lixun910@gmail.com>-
-- Date: 2021-4-21
-- Changes:
-- 2021-4-21 Expose queen_weights() as the major interface for queen weights creation
--------------------------------------

-------------------------------------------------------------------
-- knn_weights('ogc_fid', 'wkb_geometry', 4, 'knn4', 'nat')
-- DEPENDENCIES
-- geoda_weights_getfids()
-- geoda_weights_toset()
-- geoda_weights_knn()
-------------------------------------------------------------------
CREATE OR REPLACE FUNCTION knn_weights(
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
    bytea_knn_geom_transfn(internal, integer, bytea, integer)
    RETURNS internal
AS 'MODULE_PATHNAME', 'bytea_knn_geom_transfn'
    LANGUAGE c PARALLEL SAFE;

CREATE OR REPLACE FUNCTION
    geom_knn_weights_bin_finalfn(internal)
    RETURNS bytea
AS 'MODULE_PATHNAME', 'geom_knn_weights_bin_finalfn'
    LANGUAGE c PARALLEL SAFE;

CREATE AGGREGATE geoda_weights_knn(integer, bytea, integer) (
    sfunc = bytea_knn_geom_transfn,
    stype = internal,
    finalfunc = geom_knn_weights_bin_finalfn
    );
