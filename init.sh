#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "user" --dbname "testdb" <<-EOSQL
    CREATE USER user;
    CREATE DATABASE testdb;
    GRANT ALL PRIVILEGES ON DATABASE testdb TO user;
EOSQL
