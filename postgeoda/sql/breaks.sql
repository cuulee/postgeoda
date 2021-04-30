-------------------------------------
-- Author: Xun Li <lixun910@gmail.com>-
-- Date: 2021-4-29
-- Changes:
--------------------------------------

-- variable_transfn()
-- common internal function used by all breaks functions
CREATE OR REPLACE FUNCTION variable_transfn(internal, anyelement)
    RETURNS internal
AS 'MODULE_PATHNAME', 'variable_transfn'
    LANGUAGE c PARALLEL SAFE;

CREATE OR REPLACE FUNCTION variable_transfn(internal, anyelement, integer)
    RETURNS internal
AS 'MODULE_PATHNAME', 'variable_transfn'
    LANGUAGE c PARALLEL SAFE;

--------------------------------------
-- hinge15_breaks(crm_prs)
-- AGGREGATE
--------------------------------------

CREATE OR REPLACE FUNCTION hinge15_finalfn(internal)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'hinge15_finalfn'
    LANGUAGE c PARALLEL SAFE;

CREATE AGGREGATE hinge15_breaks(anyelement) (
    sfunc = variable_transfn,
    stype = internal,
    finalfunc = hinge15_finalfn
    );

--------------------------------------
-- hinge30_breaks(crm_prs)
-- AGGREGATE
--------------------------------------

CREATE OR REPLACE FUNCTION hinge30_finalfn(internal)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'hinge30_finalfn'
    LANGUAGE c PARALLEL SAFE;

CREATE AGGREGATE hinge30_breaks(anyelement) (
    sfunc = variable_transfn,
    stype = internal,
    finalfunc = hinge30_finalfn
    );

--------------------------------------
-- percentile_breaks(crm_prs)
-- AGGREGATE
--------------------------------------

CREATE OR REPLACE FUNCTION percentile_finalfn(internal)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'percentile_finalfn'
    LANGUAGE c PARALLEL SAFE;

CREATE AGGREGATE percentile_breaks(anyelement) (
    sfunc = variable_transfn,
    stype = internal,
    finalfunc = percentile_finalfn
    );

--------------------------------------
-- stddev_breaks(crm_prs)
-- AGGREGATE
--------------------------------------

CREATE OR REPLACE FUNCTION stddev_finalfn(internal)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'stddev_finalfn'
    LANGUAGE c PARALLEL SAFE;

CREATE AGGREGATE stddev_breaks(anyelement) (
    sfunc = variable_transfn,
    stype = internal,
    finalfunc = stddev_finalfn
    );

--------------------------------------
-- quantile_breaks(k, crm_prs)
-- AGGREGATE
--------------------------------------

CREATE OR REPLACE FUNCTION quantile_finalfn(internal)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'quantile_finalfn'
    LANGUAGE c PARALLEL SAFE;

CREATE AGGREGATE quantile_breaks(anyelement, integer) (
    sfunc = variable_transfn,
    stype = internal,
    finalfunc = quantile_finalfn
    );

--------------------------------------
-- natural_breaks(k, crm_prs)
-- AGGREGATE
--------------------------------------

CREATE OR REPLACE FUNCTION naturalbreaks_finalfn(internal)
    RETURNS float8[]
AS 'MODULE_PATHNAME', 'naturalbreaks_finalfn'
    LANGUAGE c PARALLEL SAFE;

CREATE AGGREGATE natural_breaks(anyelement, integer) (
    sfunc = variable_transfn,
    stype = internal,
    finalfunc = naturalbreaks_finalfn
    );
