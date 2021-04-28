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

CREATE OR REPLACE FUNCTION local_moran(anyelement, bytea, integer, character varying, numeric, integer, integer)
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
