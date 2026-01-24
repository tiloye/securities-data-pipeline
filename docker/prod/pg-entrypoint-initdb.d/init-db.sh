#!/usr/bin/env bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE "prefect";
    CREATE DATABASE "metabase";
    CREATE DATABASE "securities_db";
    GRANT ALL PRIVILEGES ON DATABASE "prefect" TO "$POSTGRES_USER";
    GRANT ALL PRIVILEGES ON DATABASE "metabase" TO "$POSTGRES_USER";
    GRANT ALL PRIVILEGES ON DATABASE "securities_db" TO "$POSTGRES_USER";
EOSQL
