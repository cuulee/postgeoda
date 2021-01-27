CREATE FUNCTION pg_all_queries(OUT query TEXT, pid OUT TEXT)
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'pg_all_queries'
LANGUAGE C STRICT VOLATILE;



-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION base36" to load this file. \quit
CREATE FUNCTION base36_encode(digits int)
RETURNS text
LANGUAGE plpgsql IMMUTABLE STRICT
  AS $$
    DECLARE
      chars char[];
      ret varchar;
      val int;
    BEGIN
      chars := ARRAY[
                '0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f','g','h',
                'i','j','k','l','m','n','o','p','q','r','s','t', 'u','v','w','x','y','z'
              ];

      val := digits;
      ret := '';

    WHILE val != 0 LOOP
      ret := chars[(val % 36)+1] || ret;
      val := val / 36;
    END LOOP;

    RETURN(ret);
    END;
  $$;

create function
    grt_sfunc( point, float8 )
    returns
        point
as
'MODULE_PATHNAME', 'grt_sfunc'
    language
        c
    immutable;

create function grt_finalfunc(agg_state point)
    returns float8
    immutable
    language plpgsql
as $$
begin
    return agg_state[1];
end;
$$;

create aggregate greatest_running_total (float8)
    (
    sfunc = grt_sfunc,
    stype = point,
    finalfunc = grt_finalfunc
    );

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
-- geoda_localmoran_b(crm_prs, bytea)
--------------------------------------
CREATE OR REPLACE FUNCTION geoda_localmoran_b(integer, anyelement, bytea)
    RETURNS point
AS 'MODULE_PATHNAME', 'local_moran_window_bytea'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- geoda_localmoran(crm_prs, bytea)
--------------------------------------
CREATE OR REPLACE FUNCTION geoda_localmoran(anyelement, bytea)
    RETURNS point
AS 'MODULE_PATHNAME', 'local_moran_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- geoda_localmoran_fast(crm_prs, bytea)
-- select "Crm_prs", wkb_geometry,
--  Array(select "Crm_prs" from guerry) as abc
-- 			  FROM guerry;
--------------------------------------
CREATE OR REPLACE FUNCTION geoda_localmoran_fast(anyelement, bytea, anyarray)
    RETURNS point
AS 'MODULE_PATHNAME', 'local_moran_fast'
    LANGUAGE 'c' PARALLEL SAFE COST 1000;

-- select ogc_fid, ARRAY["Crm_prs", "Crm_prp"] From guerry;

--------------------------------------
-- deprecated: geoda_weights_at()
--------------------------------------
CREATE OR REPLACE FUNCTION geoda_weights_at(integer, bytea)
    RETURNS bytea
AS 'MODULE_PATHNAME', 'PGWeight_at'
    LANGUAGE C PARALLEL SAFE
               COST 100;

--------------------------------------
-- deprecated: geoda_queenweights_set()
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
