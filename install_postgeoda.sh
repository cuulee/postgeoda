#!/bin/bash
#
# Init script to success tests.
#
createdb -E UTF8 test; 
psql -d test -c "CREATE EXTENSION postgis";
psql -d test -c "CREATE EXTENSION postgeoda";
