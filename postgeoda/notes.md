
## Postgresql Server

### 1. Install Postgres 12 on Mac osx

```
Installation Directory: /Library/PostgreSQL/12
Server Installation Directory: /Library/PostgreSQL/12
Data Directory: /Library/PostgreSQL/12/data
Database Port: 5432
Database Superuser: postgres
Operating System Account: postgres
Database Service: postgresql-12
Command Line Tools Installation Directory: /Library/PostgreSQL/12
pgAdmin4 Installation Directory: /Library/PostgreSQL/12/pgAdmin 4
Stack Builder Installation Directory: /Library/PostgreSQL/12
````

### 2.Install PostGIS on Ubuntu 18.04
```bash
sudo apt install postgis postgresql-10-postgis-2.4
```

### 3. Create user in PG

```
sudo su - postgres -c "createuser john"
```

* ERROR:  permission denied to create database

```shell
alter user xun with createdb;
```

* ERROR:  permission denied to set parameter "lc_messages"

```
ASSIGNING SUPERUSER PERMISSION

ALTER USER xun WITH SUPERUSER;
```

* Test

```
sudo su - postgres -c "psql -f /home/xun/Downloads/postgeoda/test/test_weights_queen.sql"
PGPASSWORD=abc123 psql -U postgres -f /Users/xun/Github/postgeoda/test/test_weights_queen.sql
```

* Install

```
sudo cmake --build /home/xun/Github/libgeoda/cmake-build-release --target install

codesign -f -s - /Library/PostgreSQL/12/lib/postgresql/postgeoda-0.0.1.so
```

### 3. Parallel

* OSX  Using Postgresql 11

```shell
sudo su - postgres
```

Update the file:  /Library/PostgreSQL/11/data/postgresql.conf
```
max_worker_processes = 8                # (change requires restart)
max_parallel_workers_per_gather = 2     # taken from max_parallel_workers
max_parallel_workers = 8                # maximum number of max_worker_processes that
```

Restart PG
```
sudo -u postgres pg_ctl -D /Library/PostgreSQL/11/data stop
sudo -u postgres pg_ctl -D /Library/PostgreSQL/11/data start
```

## Development

1. Compile postgeoda, sql
2. Run ./install.sh
3. Disconnect server
4. Reconnect server

### 1. To install postgeoda in PG:

```SQL
DROP EXTENSION IF EXISTS postgeoda;
CREATE EXTENSION postgeoda;
```

### 2. Create a spatial weights

* Create a spatial weights (I)

```SQL
SELECT geoda_weights_queen(ogc_fid, wkb_geometry) FROM nat;
-- show weights as json
SELECT geoda_weights_tojson(geoda_weights_queen(ogc_fid, wkb_geometry)) FROM nat;
```

* Store binary weights into a table (< 1million observation)

```SQL
INSERT INTO weights
SELECT
	'nat_queen' as id,
	geoda_weights_queen(ogc_fid, wkb_geometry) AS w
FROM nat;
```

* Create a spatial weights (II) 

Create a spatial weights and store to column `queen_w`:

```SQL
SELECT geoda_weights_queen('ogc_id', 'wkb_geometry', 'queen_w', 'nat');
```

```SQL
SELECT geoda_weights_astext(queen_w) FROM nat;
```

What's under the hood:

```SQL
WITH tmp_w AS (
	SELECT
		geoda_weights_queen(ogc_fid, wkb_geometry) AS w
	FROM nat
)
UPDATE nat ta SET queen_w = tb.w
FROM (
	SELECT
		geoda_weights_getfids(w) AS fid,
		geoda_weights_toset(w) AS wa
	FROM tmp_w
) AS tb
WHERE ta.ogc_fid = tb.fid;
```

```SQL
INSERT INTO weights
SELECT
	'nat_queen' as id,
	geoda_weights_queen(ogc_fid, wkb_geometry) AS w
FROM nat;

SELECT
	ogc_fid,
	geoda_localmoran_b(
		ogc_fid,
		"hr60",
		(SELECT geoda_weights_queen(ogc_fid, wkb_geometry) FROM nat)
	) OVER ()
FROM nat;
```

```SQL
-- show weights as json
SELECT geoda_weights_tojson(w) FROM nat_queen;
-- show weights as text
SELECT geoda_weights_astext((geoda_weights_toset(w)) FROM nat_queen;
```

### 3. Distance-based Spatial Weights

PostGIS provides some functions that can be utilized to create distance-based weights.

First, create a spatial index to get a better performance:

```SQL
CREATE INDEX sdoh_geom_idx
  ON sdoh
  USING GIST (geom);
```

Second, get the minimum pairwise distance among the geometries:
```SQL
SELECT 
	ST_Distance(t1.geom, t2.geom) 
FROM sdoh t2, sdoh t1 
WHERE t2.gid <> t1.gid ORDER BY ST_Distance(t1.geom, t2.geom) LIMIT 1
```

NOTE: the above SQL is very slow on large dataset. It took minutes to run. The problem is
`ST_Distance(t1.geom, t2.geom)` in WHERE clause will compute the actual distance for all 
pairs, which is not necessary at all in this task. 

PostGeoDa provides a much faster SQL function `min_distthreshold()` to fulfil the same task. Instead of compute the actual distance for all pairs, this function will use the spatial index to
find the nearest neighbor for each observation, then compute the distance and pick the smallest
distance:

```SQL
SELECT min_distthreshold(gid, geom) FROM sdoh
-- output
-- 0.916502
```

Last, using the minimum pairwise distance, we can create a distance-based spatial weights:

```SQL
SELECT gid, distance_weights(gid, geom, 0.916502) OVER() FROM sdoh
````

### 4. LISA

* Local Moran using binary weights (window function)

```SQL
SELECT queen_weights('ogc_fid', 'wkb_geometry', 'queen_w', 'nat');
SELECT local_moran(hr60, queen_w) OVER(ORDER BY ogc_fid) FROM nat;
```

```SQL
SELECT ogc_fid, knn_weights(ogc_fid, wkb_geometry, 4) OVER() FROM nat;
```

```SQL
--create a knn weights and save it to column "knn4"
ALTER TABLE nat ADD COLUMN IF NOT EXISTS knn4 bytea;
UPDATE nat SET knn4 = w.knn_weights
FROM (SELECT ogc_fid, knn_weights(ogc_fid, wkb_geometry, 4) OVER() FROM nat) as w
WHERE nat.ogc_fid = w.ogc_fid
```

```SQL
SELECT local_g(hr60, knn4) OVER() FROM nat;
```

```SQL
--do weights creation + LISA in single query
SELECT 
	local_moran(hr60, knn_weights) OVER()
FROM 
	(SELECT hr60, knn_weights(ogc_fid, wkb_geometry, 4) OVER() FROM nat) 
AS lisa
```

## Logs

### 4/1/2021

For brew installed postgresql:

```SQL
pg_ctl -D /opt/homebrew/var/postgresql@11 start
```
abc123 pwd

### 4/5/2021

add local_joincount, local_bijoincount, local_mulitijoincount
add local_geary, local_multigeary
add quantile_lisa
add neighbor_match_test
add skater
add schc
add redcap
add azp_sa
add azp_tabu
add azp_greedy
add maxp_sa
add maxp_tabu
add maxp_greedy

## Weights SQL functions

The weights creation SQL functions staring with `geoda_` are Aggregate functions. Other weights creation SQL functions are Window functions. 

The aggregate weights function can return the weights as a whole in the format of BYTEA, while
the window weights function can store the weights in a BYTEA column for each observation separately. 

### Aggregate weights functions

Each aggregate weights creation function depends on two internal functions that are defined as 
(1) sfunc and (2) finalfunc. The `sfunc` is used to collect or aggregate all geometries, then
the `finalfunc` is used to process the geometries and create a spatial weights.

For example:

```SQL
CREATE AGGREGATE geoda_weights_cont(fid integer, geom bytea, is_queen boolean)(
    sfunc = bytea_to_geom_transfn,
    stype = internal,
    finalfunc = geom_to_contweights_finalfn
    );
```

dependes on sfunc:
```SQL
CREATE OR REPLACE FUNCTION bytea_to_geom_transfn(
    internal, integer, bytea, boolean
)
    RETURNS internal
AS 'MODULE_PATHNAME', 'bytea_to_geom_transfn'
    LANGUAGE c PARALLEL SAFE;
```

and `finalfunc`:
```SQL
CREATE OR REPLACE FUNCTION
    geom_to_contweights_finalfn(internal)
    RETURNS bytea
AS 'MODULE_PATHNAME', 'geom_to_contweights_finalfn'
    LANGUAGE c PARALLEL SAFE;
```

The `sfunc` stores the interim data, e.g. all geometries, in a data structure using 
`MemoryContext` e.g.

```c
typedef struct CollectionBuildState
{
    List *geoms;  /* collected geometries */
    List *ogc_fids;
    bool is_queen;
    int order;
    bool inc_lower;
    double precision_threshold;
    Oid geomOid;
} CollectionBuildState;
```

This `MemoryContext` is an aggregate context, and it will be shared with the `finalfunc`. 


### Window weights function

The window weights function is deisgned to create spatial weights for each observation
separately. 

```SQL
CREATE OR REPLACE FUNCTION queen_weights(integer, bytea)
	RETURNS bytea 
AS 'MODULE_PATHNAME', 'pg_queen_weights_window'
```

The C function `pg_queen_weights_window` will (1) collect the geometries in the window via
OVER() clause, (2) create a spatial weights as a whole, (3) return the spatial weights
for each observation

## LISA SQL functions

All LISA SQL functions are Window functions, not Aggregate functions.
The LISA function performs a calculation across a set of table rows, and
return results of same number of rows. Therefore, the LISA SQL query should
always contains an OVER cluase firectly following the window function's name
and arguments. The OVER clause determines exactly how the rows of the query
are split up for processing by the window function.

### Return type of LISA SQL functions

Todo: change return type of LISA SQL calls to ARRAY. See: https://www.postgresql.org/docs/10/arrays.html

For example, the return value of geoda_localmoran() should be an array
for each row: {0.365282, 0.244}, instead of using `POINT` as the return type.

## The calling sequences of LISA SQL functions

1. SQL definition

```SQL
CREATE OR REPLACE FUNCTION geoda_quantilelisa(integer, integer, anyelement, bytea)
	RETURNS point
AS 'MODULE_PATHNAME', 'pg_quantilelisa_window'
```

2. C function implementation

quantilelisa.c

```c
Datum pg_quantilelisa_window(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pg_quantilelisa_window);
Datum quantilelisa_window(PG_FUNCTION_ARGS) {
	// call libgeoda
	// pg_quantilelisa()
}
```

3. Proxy to libgeoda

proxy.h/cpp

```c
Point** pg_quantilelisa()
```

scratch pad

SELECT min_distthreshold(gid, geom) FROM sdoh

SELECT distance_weights(gid, geom, 0.916502, 2, false, false, false) OVER() FROM sdoh;

SELECT knn_weights(gid, geom, 4, 1.0, FALSE, FALSE) OVER() FROM sdoh

SELECT kernel_knn_weights(gid, geom, 4, 'gaussian') OVER() FROM sdoh

SELECT kernel_knn_weights(gid, geom, 4, 'gaussian', 1.0, FALSE, FALSE, FALSE, FALSE, FALSE) OVER() FROM sdoh

SELECT kernel_weights(gid, geom, 0.916502, 'gaussian') OVER() FROM sdoh

SELECT queen_weights(gid, geom) OVER() FROM sdoh;

ALTER TABLE sdoh ADD COLUMN IF NOT EXISTS queen_w bytea;

UPDATE sdoh SET queen_w = w.queen_weights
FROM (SELECT gid, queen_weights(gid, geom) OVER() FROM sdoh) as w
WHERE sdoh.gid = w.gid

SELECT local_moran(ep_pov::real, queen_w) OVER() FROM sdoh;

WITH tmp AS (
SELECT gid, ep_pov, queen_weights(gid, geom) OVER() FROM sdoh
) SELECT gid, local_moran(ep_pov::real, queen_weights) OVER() FROM tmp

SELECT gid, local_moran(ep_pov::real, queen_weights) OVER() FROM
( SELECT gid, ep_pov, queen_weights(gid, geom) OVER() FROM sdoh) AS tmp

--save result
ALTER TABLE sdoh ADD COLUMN IF NOT EXISTS lmq float8[];

UPDATE sdoh SET lmq = tmp.local_moran
FROM (
SELECT gid, local_moran(ep_pov::real, queen_w) OVER() FROM sdoh
) AS tmp
WHERE sdoh.gid = tmp.gid

SELECT neighbor_match_test(geom, ARRAY[ep_pov::real, ep_pci::real], 4) OVER() FROM sdoh;

SELECT neighbor_match_test(geom, ARRAY[ep_pov::real, ep_pci::real], 4, 1.0,
FALSE, FALSE, FALSE, 'standardize', 'euclidean') OVER() FROM sdoh;

--SELECT hinge15_breaks(ep_pov::real) FROM sdoh;
--SELECT percentile_breaks(ep_pov::real) FROM sdoh;
--SELECT stddev_breaks(ep_pov::real) FROM sdoh;
--SELECT quantile_breaks(ep_pov::real, 5) FROM sdoh;

SELECT skater(5, ARRAY[ep_pov::real, ep_unem::real], queen_w) OVER() FROM sdoh;

56,861,745
56Million

20GB memory
```SQL
update nets2014_misc SET women = CASE
WHEN womenowned = 'Y' THEN 1 ELSE 0 END
```
UPDATE 60061744
Query returned successfully in 14 min 16 secs.


```SQL
CREATE INDEX nets_geom_idx ON nets2014_misc USING GIST (geom);
CREATE INDEX nnfid_idx ON tmp_10nn (ogc_fid)
CREATE INDEX nnfid2_idx ON tmp2_10nn (ogc_fid)
```

* 1Million

-- Create Weights
SELECT 999999
Query returned successfully in 23 secs 124 msec.A

-- local_joincount
Successfully run. Total query runtime: 45 secs 394 msec.
999999 rows affected.

* 5Million

-- Create Weights
SELECT 4953899
Query returned successfully in 2 min 8 secs.

-- local_joincount
Successfully run. Total query runtime: 1 min 37 secs.
4953899 rows affected.

* 10Million

```SQL
CREATE TABLE tmp_10nn AS (
	SELECT ogc_fid, knn_weights(ogc_fid, geom, 10) OVER() FROM nets2014_misc where ogc_fid < 10000000
)
CREATE INDEX nnfid_idx ON tmp_10nn (ogc_fid)
```
SELECT 9953899
Query returned successfully in 3 min 25 secs.

```SQL
SELECT local_moran(a.women, b.knn_weights) OVER() FROM nets2014_misc AS a, tmp_10nn AS b WHERE a.ogc_fid=b.ogc_fid;
```
Successfully run. Total query runtime: 3 min 52 secs.
9953899 rows affected.

```SQL
SELECT local_joincount(a.women, b.knn_weights) OVER() FROM nets2014_misc AS a, tmp_10nn AS b WHERE a.ogc_fid=b.ogc_fid;
```
Successfully run. Total query runtime: 3 min 5 secs.
9953899 rows affected.

* 20 Million

```SQL
CREATE TABLE tmp2_10nn AS (
    SELECT ogc_fid, knn_weights(ogc_fid, geom, 10) OVER() FROM nets2014_misc where ogc_fid < 20000000
)
CREATE INDEX nnfid2_idx ON tmp2_10nn (ogc_fid)
```
SELECT 19953899
Query returned successfully in 8 min 18 secs..

```SQL
SELECT local_joincount(a.women, b.knn_weights) OVER() FROM nets2014_misc AS a, tmp2_10nn AS b WHERE a.ogc_fid=b.ogc_fid;
```
Successfully run. Total query runtime: 8 min 44 secs.
19953899 rows affected.

* 40 Million

```SQL
CREATE TABLE tmp4_10nn AS (
    SELECT ogc_fid, knn_weights(ogc_fid, geom, 10) OVER() FROM nets2014_misc where ogc_fid < 40000000
)
CREATE INDEX nnfid4_idx ON tmp4_10nn (ogc_fid)
```
SELECT 39320891
Query returned successfully in 19 min 2 secs.

* Import data

```bash
ogr2ogr -f PostgreSQL PG:"host=localhost user=postgres dbname=test password=abc123" NETS2014_Misc.csv -oo AUTODETECT_TYPE=YES -oo X_POSSIBLE_NAMES=longitude -oo Y_POSSIBLE_NAMES=latitude
```

CREATE INDEX nets_geom_idx ON nets2014_misc USING GIST (wkb_geometry);
CREATE INDEX

Query returned successfully in 1 hr 22 min.


* cartodb vm

/etc/init.d/postgresql restart

/var/log/postgresql-10-main.log