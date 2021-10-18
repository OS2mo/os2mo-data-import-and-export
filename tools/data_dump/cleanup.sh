#!/bin/bash
set -eou pipefail

DB_CONTAINER="$1"
DB_NAME="$2"

echo "Cleaning up $DB_NAME"
docker exec -t -u postgres "${DB_CONTAINER}" psql -c \
    "drop database if exists ${DB_NAME}_old"
