-------------------------------------
-- Author: Xun Li <lixun910@gmail.com>-
-- Date: 2021-4-27
-- Changes:
-- 2021-4-27 add local_joincount(), add local_bijoincount(), local_multijoincount()
-- 2021-4-28 add full version of the 3 query functions
--------------------------------------

--------------------------------------
-- local_joincount(crm_prs, bytea)
--------------------------------------
CREATE OR REPLACE FUNCTION local_joincount(anyelement, bytea)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_joincount_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

CREATE OR REPLACE FUNCTION local_joincount(anyelement, bytea, integer, character varying, numeric, integer, integer)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_joincount_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- local_bijoincount(crm_prs, litercy, bytea)
--------------------------------------
CREATE OR REPLACE FUNCTION local_bijoincount(anyelement, anyelement, bytea)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_bijoincount_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

CREATE OR REPLACE FUNCTION local_bijoincount(anyelement, anyelement, bytea, integer, character varying, numeric, integer, integer)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_bijoincount_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- local_multijoincount(ARRAY["Crm_prs", "Crm_prp"], bytea)
--------------------------------------
CREATE OR REPLACE FUNCTION local_multijoincount(anyarray, bytea)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_multijoincount_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

CREATE OR REPLACE FUNCTION local_multijoincount(anyarray, bytea, integer, character varying, numeric, integer, integer)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_multijoincount_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;
