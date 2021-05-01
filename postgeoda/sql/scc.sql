-------------------------------------
-- Author: Xun Li <lixun910@gmail.com>-
-- Date: 2021-4-30
-- Changes:
-- 2021-4-30 add skater(), add schc(), redcap()
--------------------------------------

--------------------------------------
-- skater(5, ARRAY["Crm_prs", "Crm_prp"], bytea)
--------------------------------------
CREATE OR REPLACE FUNCTION skater(integer, anyarray, bytea)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_skater1_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- skater(5, ARRAY["Crm_prs", "Crm_prp"], bytea, min_region_size=10)
--------------------------------------
CREATE OR REPLACE FUNCTION skater(integer, anyarray, bytea, integer)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_skater1_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- skater(5, ARRAY["Crm_prs", "Crm_prp"], bytea, min_region_size=10, scale_method, distance_type, seed, cpu_threads)
--------------------------------------
CREATE OR REPLACE FUNCTION skater(
    integer, anyarray, bytea, integer, character varying, character varying, integer, integer
)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_skater1_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- skater(5, ARRAY["Crm_prs", "Crm_prp"], bytea, min_bound=Pop1831, min_bound_val=3236.67)
--------------------------------------
CREATE OR REPLACE FUNCTION skater(integer, anyarray, bytea, integer, anyelement, float)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_skater2_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- skater(5, ARRAY["Crm_prs", "Crm_prp"], bytea, min_bound=Pop1831, min_bound_val=3236.67, scale_method, distance_type, seed, cpu_threads)
--------------------------------------
CREATE OR REPLACE FUNCTION skater(
    integer, anyarray, bytea, anyelement, float, character varying, character varying, integer, integer
)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_skater2_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;
