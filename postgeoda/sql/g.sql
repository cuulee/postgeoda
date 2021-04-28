-------------------------------------
-- Author: Xun Li <lixun910@gmail.com>-
-- Date: 2021-4-27
-- Changes:
-- 2021-4-27 add local_g(), local_gstar()
--------------------------------------

--------------------------------------
-- local_g_b(ogc_fid, crm_prs, bytea)
-- the weights should be passed as a whole in BYTEA format
-- (This function will be depcreciated)
--------------------------------------
CREATE OR REPLACE FUNCTION geoda_localg_b(integer, anyelement, bytea)
    RETURNS point
AS 'MODULE_PATHNAME', 'local_g_window_bytea'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- local_gstar_b(ogc_fid, crm_prs, bytea)
-- the weights should be passed as a whole in BYTEA format
-- (This function will be depcreciated)
--------------------------------------
CREATE OR REPLACE FUNCTION geoda_localgstar_b(integer, anyelement, bytea)
    RETURNS point
AS 'MODULE_PATHNAME', 'local_gstar_window_bytea'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;
