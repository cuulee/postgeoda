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
