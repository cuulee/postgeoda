-------------------------------------
-- Author: Xun Li <lixun910@gmail.com>-
-- Date: 2021-4-30
-- Changes:
-- 2021-5-1 add redcap()
--------------------------------------

--------------------------------------
-- redcap(5, ARRAY["Crm_prs", "Crm_prp"], bytea, "firstorder-singlelinkage")
--------------------------------------
CREATE OR REPLACE FUNCTION redcap(integer, anyarray, bytea, character varying)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_redcap1_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- redcap(5, ARRAY["Crm_prs", "Crm_prp"], bytea, "firstorder-singlelinkage", min_region_size=10)
--------------------------------------
CREATE OR REPLACE FUNCTION redcap(integer, anyarray, bytea, character varying, integer)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_redcap1_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- redcap(5, ARRAY["Crm_prs", "Crm_prp"], bytea, "firstorder-singlelinkage", min_region_size=10, scale_method, distance_type, seed, cpu_threads)
--------------------------------------
CREATE OR REPLACE FUNCTION redcap(
    integer, anyarray, bytea, character varying, integer, character varying, character varying, integer, integer
)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_redcap1_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- redcap(5, ARRAY["Crm_prs", "Crm_prp"], bytea, "firstorder-singlelinkage", min_bound=Pop1831, min_bound_val=3236.67)
--------------------------------------
CREATE OR REPLACE FUNCTION redcap(integer, anyarray, bytea, character varying, anyelement, float)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_redcap2_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

CREATE OR REPLACE FUNCTION redcap(integer, anyarray, bytea, character varying, bigint, float)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_redcap2_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- redcap(5, ARRAY["Crm_prs", "Crm_prp"], bytea, "firstorder-singlelinkage", min_bound=Pop1831, min_bound_val=3236.67, scale_method, distance_type, seed, cpu_threads)
--------------------------------------
CREATE OR REPLACE FUNCTION redcap(
    integer, anyarray, bytea, character varying, anyelement, float, character varying, character varying, integer, integer
)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_redcap2_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

CREATE OR REPLACE FUNCTION redcap(
    integer, anyarray, bytea, character varying, bigint, float, character varying, character varying, integer, integer
)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_redcap2_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;


--------------------------------------
-- redcap(ARRAY["Crm_prs", "Crm_prp"], bytea, "firstorder-singlelinkage", min_bound=Pop1831, min_bound_val=3236.67)
--------------------------------------
CREATE OR REPLACE FUNCTION redcap(anyarray, bytea, character varying, anyelement, float)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_redcap3_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

CREATE OR REPLACE FUNCTION redcap(anyarray, bytea, character varying, bigint, float)
    RETURNS integer
AS 'MODULE_PATHNAME', 'pg_redcap3_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;
