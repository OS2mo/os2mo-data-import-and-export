#!/bin/bash
set -eou pipefail

CONTAINER_NAME="$1"
DB_NAME="$2"
BACKUP_DIR="$3"

BACKUP_FILE=$BACKUP_DIR/backup_$DB_NAME.pgdump


echo "Snapshotting ${DB_NAME}"
docker exec -t -u postgres "${CONTAINER_NAME}" /bin/bash -c \
    "pg_dump -Fc -f $BACKUP_FILE $DB_NAME"

_exit="$?"
if [ "$_exit" -ne 0 ]; then
    echo "Failed to snapshot database $DB_NAME in $CONTAINER_NAME"
    exit 1
fi
echo "Snapshot of $DB_NAME successfully saved."