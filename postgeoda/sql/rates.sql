-------------------------------------
-- Author: Xun Li <lixun910@gmail.com>-
-- Date: 2021-5-7
--------------------------------------

--------------------------------------
-- excess_risk(event_variable, base_variable)
--------------------------------------
CREATE OR REPLACE FUNCTION excess_risk(anyelement, anyelement)
    RETURNS real[]
AS 'MODULE_PATHNAME', 'pg_excess_risk'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

