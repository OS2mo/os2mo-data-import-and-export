#!/bin/bash

OIOREST_CONTAINER_NAME=${OIOREST_CONTAINER_NAME:-"mox_service"}
CONTAINER_NAME=${CONTAINER_NAME:-"mox_database"}

if ! [ -x "$(command -v docker)" ]; then
    echo "Unable to locate the 'docker' executable."
    exit 1
fi
if [ ! "$(docker ps -q -f name=${CONTAINER_NAME})" ]; then
    echo "Unable to locate a running mox database container: ${CONTAINER_NAME}"
    exit 1
fi

## Create backup
DATABASE_NAME=${DATABASE_NAME:-"mox"}
DOCKER_SNAPSHOT_DESTINATION=${DOCKER_SNAPSHOT_DESTINATION:-"/database_snapshot/os2mo_database.sql"}
echo "Restoring ${DATABASE_NAME}"
docker exec -t "${OIOREST_CONTAINER_NAME}" python3 -m oio_rest truncatedb
docker exec -t "${CONTAINER_NAME}" \
        su --shell /bin/bash \
           --command "psql ${DATABASE_NAME} < ${DOCKER_SNAPSHOT_DESTINATION}" \
           postgres

# Create confdb
CONFDB_DATABASE_NAME=${CONFDB_DATABASE_NAME:-"mora"}
CONFDB_DOCKER_SNAPSHOT_DESTINATION=${CONFDB_DOCKER_SNAPSHOT_DESTINATION:-"/database_snapshot/confdb.sql"}
echo "Restoring ${CONFDB_DATABASE_NAME}"
docker exec -t "${CONTAINER_NAME}" \
        su --shell /bin/bash \
           --command "echo 'TRUNCATE orgunit_settings' | psql ${CONFDB_DATABASE_NAME}" \
           postgres
docker exec -t "${CONTAINER_NAME}" \
        su --shell /bin/bash \
           --command "psql ${CONFDB_DATABASE_NAME} < ${CONFDB_DOCKER_SNAPSHOT_DESTINATION}" \
           postgres


# Create sessions
SESSIONS_DATABASE_NAME=${SESSIONS_DATABASE_NAME:-"sessions"}
SESSIONS_DOCKER_SNAPSHOT_DESTINATION=${SESSIONS_DOCKER_SNAPSHOT_DESTINATION:-"/database_snapshot/sessions.sql"}
echo "Restoring ${SESSIONS_DATABASE_NAME}"
docker exec -t "${CONTAINER_NAME}" \
        su --shell /bin/bash \
           --command "echo 'TRUNCATE sessions' | psql ${SESSIONS_DATABASE_NAME}" \
           postgres
docker exec -t "${CONTAINER_NAME}" \
        su --shell /bin/bash \
           --command "psql ${SESSIONS_DATABASE_NAME} < ${SESSIONS_DOCKER_SNAPSHOT_DESTINATION}" \
           postgres
