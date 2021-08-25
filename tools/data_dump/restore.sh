#!/bin/bash
set -ou pipefail

DB_CONTAINER="$1"
DB_NAME="$2"
BACKUP_DIR="$3"

BACKUP_FILE=$BACKUP_DIR/backup_$DB_NAME.pgdump

echo "Renaming $DB_NAME to ${DB_NAME}_old"
docker exec -t -u postgres "${DB_CONTAINER}" psql -c \
    "alter database $DB_NAME rename to ${DB_NAME}_old"

alter_db=$?
if [ $alter_db -ne 0 ]; then
    echo "Failed to rename $DB_NAME to ${DB_NAME}_old."
    exit 1
fi

echo "Restoring $DB_NAME"
docker exec -t -u postgres "${DB_CONTAINER}" /bin/bash -c \
    "pg_restore -d postgres -C -j$(nproc) --verbose $BACKUP_FILE" >> /tmp/pg_restore.log

pg_restore=$?
if [ $pg_restore -ne 0 ]; then
    echo "pg_restore failed"
    echo "Renaming ${DB_NAME}_old back to $DB_NAME"
    docker exec -t -u postgres "${DB_CONTAINER}" psql -c \
    "alter database ${DB_NAME}_old rename to $DB_NAME"
    exit 1
fi

echo "$DB_NAME successfully restored from $BACKUP_FILE"
