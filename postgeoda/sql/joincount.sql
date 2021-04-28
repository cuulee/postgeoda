-------------------------------------
-- Author: Xun Li <lixun910@gmail.com>-
-- Date: 2021-4-27
-- Changes:
-- 2021-4-27 add local_joincount()
--------------------------------------

--------------------------------------
-- local_joincount_b(ogc_fidcrm_prs, bytea)
-- the weights should be passed as a whole in BYTEA format
-- (This function will be depcreciated)
--------------------------------------
CREATE OR REPLACE FUNCTION geoda_localjoincount_b(integer, anyelement, bytea)
    RETURNS point
AS 'MODULE_PATHNAME', 'local_joincount_window_bytea'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- local_bijoincount_b(ogc_fid, crm_prs, litercy, bytea)
-- the weights should be passed as a whole in BYTEA format
-- (This function will be depcreciated)
--------------------------------------
CREATE OR REPLACE FUNCTION geoda_blocaljoincount_b(integer, anyelement, anyelement, bytea)
    RETURNS point
AS 'MODULE_PATHNAME', 'local_bjoincount_window_bytea'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- local_multijoincount_b(ogc_fid, ARRAY["Crm_prs", "Crm_prp"], bytea)
-- the weights should be passed as a whole in BYTEA format
-- (This function will be depcreciated)
--------------------------------------
CREATE OR REPLACE FUNCTION geoda_mlocaljoincount_b(integer, anyarray, bytea)
    RETURNS point
AS 'MODULE_PATHNAME', 'local_mjoincount_window_bytea'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;
