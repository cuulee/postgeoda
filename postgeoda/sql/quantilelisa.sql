-------------------------------------
-- Author: Xun Li <lixun910@gmail.com>-
-- Date: 2021-4-27
-- Changes:
-- 2021-4-27 add local_quantilelisa(), local_multiquantilelisa()
--------------------------------------

--------------------------------------
-- local_quantilelisa(5, 1, crm_prs, bytea)
--------------------------------------
CREATE OR REPLACE FUNCTION local_quantilelisa(integer, integer, anyelement, bytea)
    RETURNS point
AS 'MODULE_PATHNAME', 'quantilelisa_window'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;
