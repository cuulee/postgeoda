
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

### 3. LISA

* Local Moran using binary weights (window function)

```SQL
SELECT queen_weights('ogc_id', 'wkb_geometry', 'queen_w', 'nat');
SELECT local_moran(hr60, queen_w) OVER(ORDER BY ogc_fid) FROM nat;
```

```SQL
SELECT knn_weights('ogc_id', 'wkb_geometry', 4, 'knn4', 'nat');
```

## Logs

### 4/1/2021

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