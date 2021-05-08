-------------------------------------
-- Author: Xun Li <lixun910@gmail.com>-
-- Date: 2021-5-7
--------------------------------------

--------------------------------------
-- excess_risk(event_variable, base_variable)
--------------------------------------
CREATE OR REPLACE FUNCTION excess_risk(anyelement, anyelement)
    RETURNS float8
AS 'MODULE_PATHNAME', 'pg_excess_risk'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- eb_rate(event_variable, base_variable)
--------------------------------------
CREATE OR REPLACE FUNCTION eb_rate(anyelement, anyelement)
    RETURNS float8
AS 'MODULE_PATHNAME', 'pg_eb_rate'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;


--------------------------------------
-- spatial_lag(variable, queen_w)
--------------------------------------
CREATE OR REPLACE FUNCTION spatial_lag(anyelement, bytea)
    RETURNS float8
AS 'MODULE_PATHNAME', 'pg_spatial_lag'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;

--------------------------------------
-- spatial_rate(variable, queen_w)
--------------------------------------
CREATE OR REPLACE FUNCTION spatial_rate(anyelement, anyelement, bytea)
    RETURNS float8
AS 'MODULE_PATHNAME', 'pg_spatial_rate'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;


--------------------------------------
-- spatial_eb(event_variable, base_variable, queen_w)
--------------------------------------
CREATE OR REPLACE FUNCTION spatial_eb(anyelement, anyelement, bytea)
    RETURNS float8
AS 'MODULE_PATHNAME', 'pg_spatial_eb'
    LANGUAGE 'c' IMMUTABLE STRICT WINDOW;
