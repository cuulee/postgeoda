-------------------------------------
-- Author: Xun Li <lixun910@gmail.com>-
-- Date: 2021-5-6
-- Changes:
-- 2021-5-6 add local_geary, local_multigeary
--------------------------------------

--------------------------------------
-- local_geary(crm_prs, queen_w)
--------------------------------------
CREATE OR REPLACE FUNCTION local_geary(anyelement, bytea)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_geary_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- local_geary(crm_prs, queen_w, 999, 'complete', 0.01, 6, 123456789)
--------------------------------------
CREATE OR REPLACE FUNCTION local_geary(
    anyelement, bytea, integer, character varying, float8, integer, integer
)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_geary_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- local_multigeary(ARRAY[ep_pov, ep_pci], queen_w)
--------------------------------------
CREATE OR REPLACE FUNCTION local_multigeary(anyarray, bytea)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_multigeary_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

CREATE OR REPLACE FUNCTION local_multigeary(
    anyarray, bytea, integer, character varying, float8, integer, integer
)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_multigeary_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;
