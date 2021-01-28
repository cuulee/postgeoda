
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
-- geoda_localjoincount_b(crm_prs, bytea)
--------------------------------------
CREATE OR REPLACE FUNCTION geoda_localjoincount_b(integer, anyelement, bytea)
    RETURNS point
AS 'MODULE_PATHNAME', 'local_joincount_window_bytea'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;