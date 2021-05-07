-------------------------------------
-- Author: Xun Li <lixun910@gmail.com>-
-- Date: 2021-4-27
-- Changes:
-- 2021-4-27 add local_quantilelisa(), local_multiquantilelisa()
-- 2021-4-28 add full version of  local_quantilelisa() and local_multiquantilelisa()
--------------------------------------

--------------------------------------
-- quantile_lisa(5, 1, crm_prs, bytea)
--------------------------------------
CREATE OR REPLACE FUNCTION quantile_lisa(integer, integer, anyelement, bytea)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_quantilelisa_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

CREATE OR REPLACE FUNCTION quantile_lisa(
    integer, integer, anyelement, bytea, integer, character varying, float8, integer, integer
)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_quantilelisa_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- multiquantile_lisa(ARRAY[5,5], ARRAY[1, 1], ARRAY[ep_pov, ep_pci], queen_w)
--------------------------------------
CREATE OR REPLACE FUNCTION multiquantile_lisa(anyarray, anyarray, anyarray, bytea)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_multiquantilelisa_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

CREATE OR REPLACE FUNCTION multiquantile_lisa(
    anyarray, anyarray, anyarray, bytea, integer, character varying, float8, integer, integer
)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'pg_local_multiquantilelisa_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;
