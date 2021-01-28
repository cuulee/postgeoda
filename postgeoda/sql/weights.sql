-------------------------------------
-- Author: Xun Li <lixun910@gmail.com>-
-- Date: 2021-1-27
-- Changes:
-- 2021-1-27 Add geoda_localjoincount_b()
--------------------------------------

-------------------------------------------------------------------
-- geoda_weights_queen('ogc_fid', 'wkb_geometry', 'queen_w', 'nat')
-- colName is the output column of weights, should be created
-- beforehand
-- DEPENDENCIES
-- geoda_weights_getfids()
-- geoda_weights_toset()
-- geoda_weights_queen()
-------------------------------------------------------------------
CREATE OR REPLACE FUNCTION geoda_weights_queen(
    fid VARCHAR, geom VARCHAR, colName VARCHAR, tableName VARCHAR
) RETURNS boolean
AS $$
BEGIN
    PERFORM '
        WITH tmp_w AS (
            SELECT
                geoda_weights_queen(' || fid || ', ' || geom || ') AS w
            FROM ' || tableName || '
        )
        UPDATE ' || tableName || ' ta SET ' || colName || ' = tb.w
        FROM (
                 SELECT
                     geoda_weights_getfids(w) AS fid,
                     geoda_weights_toset(w) AS w
                 FROM tmp_w
             ) AS tb
        WHERE ta.'||fid||' = tb.fid';
    RETURN true;
END;
$$ LANGUAGE 'plpgsql';

--------------------------------------
-- geoda_weights_queen(ogc_fid, wkb_geometry)
-- AGGREGATE
-- DEPENDENCIES
-- bytea_to_geom_transfn()
-- geom_to_queenweights_finalfn()
-- geoda_weights_queen()
--------------------------------------
CREATE OR REPLACE FUNCTION bytea_to_geom_transfn(
    internal,
    integer default null,
    bytea default null,
    integer default 1,
    boolean default false,
    float4 default 0
)
    RETURNS internal
AS 'MODULE_PATHNAME', 'bytea_to_geom_transfn'
    LANGUAGE c PARALLEL SAFE;

CREATE OR REPLACE FUNCTION
    bytea_to_geom_transfn(internal, integer, bytea)
    RETURNS internal
AS 'MODULE_PATHNAME', 'bytea_to_geom_transfn'
    LANGUAGE c PARALLEL SAFE;

CREATE OR REPLACE FUNCTION
    geom_to_queenweights_finalfn(internal)
    RETURNS bytea
AS 'MODULE_PATHNAME', 'geom_to_queenweights_finalfn'
    LANGUAGE c PARALLEL SAFE;

CREATE AGGREGATE geoda_weights_queen(integer, bytea, integer, boolean, float4) (
    sfunc = bytea_to_geom_transfn,
    stype = internal,
    finalfunc = geom_to_queenweights_finalfn
    );

CREATE AGGREGATE geoda_weights_queen(fid integer, geom bytea)(
    sfunc = bytea_to_geom_transfn,
    stype = internal,
    finalfunc = geom_to_queenweights_finalfn
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
-- geoda_weights_astext(bytea)
-- bytea of weights of ONE observation
--------------------------------------
CREATE OR REPLACE FUNCTION geoda_weights_astext(bytea)
    RETURNS text
AS 'MODULE_PATHNAME', 'weights_to_text'
    LANGUAGE c PARALLEL SAFE
    COST 100;

--------------------------------------
-- geoda_weights_knn(ogc_fid, wkb_geometry, k=4)
-- AGGREGATE function
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

--------------------------------------
-- DEPRECATED geoda_queenweights(wkb_geometry)
--------------------------------------
CREATE OR REPLACE FUNCTION geoda_queenweights(integer, bytea, int)
    RETURNS bytea
AS 'MODULE_PATHNAME', 'weights_queen_window'
    LANGUAGE 'c' STRICT WINDOW;
