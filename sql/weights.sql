CREATE FUNCTION pg_all_queries(OUT query TEXT, pid OUT TEXT)
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'pg_all_queries'
LANGUAGE C STRICT VOLATILE;

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION base36" to load this file. \quit
CREATE FUNCTION base36_encode(digits int)
RETURNS text
LANGUAGE plpgsql IMMUTABLE STRICT
  AS $$
    DECLARE
      chars char[];
      ret varchar;
      val int;
    BEGIN
      chars := ARRAY[
                '0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f','g','h',
                'i','j','k','l','m','n','o','p','q','r','s','t', 'u','v','w','x','y','z'
              ];

      val := digits;
      ret := '';

    WHILE val != 0 LOOP
      ret := chars[(val % 36)+1] || ret;
      val := val / 36;
    END LOOP;

    RETURN(ret);
    END;
  $$;


CREATE OR REPLACE FUNCTION
bytea_to_geom_transfn(point, bytea)
RETURNS point 
AS 'MODULE_PATHNAME', 'bytea_to_geom_transfn'
LANGUAGE c PARALLEL SAFE;

CREATE OR REPLACE FUNCTION
bytea_to_geom_finalfn(point)
RETURNS integer
AS 'MODULE_PATHNAME', 'bytea_to_geom_finalfn'
LANGUAGE c PARALLEL SAFE;

CREATE AGGREGATE weights_queen(bytea) (
  sfunc = bytea_to_geom_transfn,
  stype = point,
  finalfunc = bytea_to_geom_finalfn
);



create function
  grt_sfunc( point, float8 )
returns
  point
as
  'MODULE_PATHNAME', 'grt_sfunc'
language
  c
immutable;

create function grt_finalfunc(agg_state point)
returns float8
immutable
language plpgsql
as $$
begin
  return agg_state[1];
end;
$$;

create aggregate greatest_running_total (float8)
(
    sfunc = grt_sfunc,
    stype = point,
    finalfunc = grt_finalfunc
);
