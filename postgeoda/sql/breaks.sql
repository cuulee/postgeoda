-------------------------------------
-- Author: Xun Li <lixun910@gmail.com>-
-- Date: 2021-4-29
-- Changes:
--------------------------------------

--------------------------------------
-- hinge15_breaks(crm_prs)
-- AGGREGATE
--------------------------------------
CREATE OR REPLACE FUNCTION variable_transfn(internal, anyelement)
    RETURNS internal
AS 'MODULE_PATHNAME', 'variable_transfn'
    LANGUAGE c PARALLEL SAFE;

CREATE OR REPLACE FUNCTION hinge15_finalfn(internal)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'hinge15_finalfn'
    LANGUAGE c PARALLEL SAFE;

CREATE AGGREGATE hinge15_breaks(anyelement) (
    sfunc = variable_transfn,
    stype = internal,
    finalfunc = hinge15_finalfn
    );

