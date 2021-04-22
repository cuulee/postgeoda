-------------------------------------
-- Author: Xun Li <lixun910@gmail.com>-
-- Date: 2021-1-27
-- Changes:
-- 2021-1-27 add geoda_localjoincount_b()
-- 2021-1-29 add bivariate geoda_blocaljoincount_b(), multivariate geoda_mlocaljoincount_b()
-- 2021-4-10 add geoda_quantilelisa
--------------------------------------

--------------------------------------
-- geoda_localmoran_b(crm_prs, bytea)
--------------------------------------
CREATE OR REPLACE FUNCTION geoda_localmoran_b(integer, anyelement, bytea)
    RETURNS point
AS 'MODULE_PATHNAME', 'local_moran_window_bytea'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- local_moran(crm_prs, bytea)
--------------------------------------
CREATE OR REPLACE FUNCTION local_moran(anyelement, bytea)
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
-- geoda_localjoincount_b(ogc_fidcrm_prs, bytea)
--------------------------------------
CREATE OR REPLACE FUNCTION geoda_localjoincount_b(integer, anyelement, bytea)
    RETURNS point
AS 'MODULE_PATHNAME', 'local_joincount_window_bytea'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- geoda_blocaljoincount_b(ogc_fid, crm_prs, litercy, bytea)
--------------------------------------
CREATE OR REPLACE FUNCTION geoda_blocaljoincount_b(integer, anyelement, anyelement, bytea)
    RETURNS point
AS 'MODULE_PATHNAME', 'local_bjoincount_window_bytea'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- geoda_mlocaljoincount_b(ogc_fid, ARRAY["Crm_prs", "Crm_prp"], bytea)
--------------------------------------
CREATE OR REPLACE FUNCTION geoda_mlocaljoincount_b(integer, anyarray, bytea)
    RETURNS point
AS 'MODULE_PATHNAME', 'local_mjoincount_window_bytea'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- geoda_localg_b(ogc_fid, crm_prs, bytea)
--------------------------------------
CREATE OR REPLACE FUNCTION geoda_localg_b(integer, anyelement, bytea)
    RETURNS point
AS 'MODULE_PATHNAME', 'local_g_window_bytea'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- geoda_localgstar_b(ogc_fid, crm_prs, bytea)
--------------------------------------
CREATE OR REPLACE FUNCTION geoda_localgstar_b(integer, anyelement, bytea)
    RETURNS point
AS 'MODULE_PATHNAME', 'local_gstar_window_bytea'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- geoda_quantilelisa(5, 1, crm_prs, bytea)
--------------------------------------
CREATE OR REPLACE FUNCTION geoda_quantilelisa(integer, integer, anyelement, bytea)
    RETURNS point
AS 'MODULE_PATHNAME', 'quantilelisa_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;
