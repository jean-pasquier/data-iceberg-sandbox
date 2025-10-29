#!/bin/bash

set -e
set -u

function create_database() {
  local database=$1
  echo "Creating database '$database'"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
        CREATE USER $database PASSWORD '$database';
        CREATE DATABASE $database;
        GRANT ALL PRIVILEGES ON DATABASE $database TO $database;
        ALTER DATABASE $database OWNER TO $database;
EOSQL
}

if [ -n "${POSTGRES_DATABASES:-}" ]; then
  echo "Multiple database creation requested: $POSTGRES_DATABASES"
  for db in $(echo $POSTGRES_DATABASES | tr ',' ' '); do
    create_database $db
  done
  echo "Databases created"
fi
