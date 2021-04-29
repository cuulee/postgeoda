-------------------------------------
-- Author: Xun Li <lixun910@gmail.com>-
-- Date: 2021-4-27
-- Changes:
-- 2021-4-27 add local_quantilelisa(), local_multiquantilelisa()
-- 2021-4-28 add full version of  local_quantilelisa() and local_multiquantilelisa()
--------------------------------------

--------------------------------------
-- local_quantilelisa(5, 1, crm_prs, bytea)
--------------------------------------
CREATE OR REPLACE FUNCTION local_quantilelisa(integer, integer, anyelement, bytea)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_quantilelisa_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

CREATE OR REPLACE FUNCTION local_quantilelisa(
    integer, integer, anyelement, bytea, integer, character varying, numeric, integer, integer
)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_quantilelisa_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- local_multiquantilelisa(ARRAY[5,5], ARRAY[1, 1], ARRAY[ep_pov, ep_pci], queen_w)
--------------------------------------
CREATE OR REPLACE FUNCTION local_multiquantilelisa(anyarray, anyarray, anyarray, bytea)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_multiquantilelisa_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

CREATE OR REPLACE FUNCTION local_multiquantilelisa(
    anyarray, anyarray, anyarray, bytea, integer, character varying, numeric, integer, integer
)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_multiquantilelisa_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;