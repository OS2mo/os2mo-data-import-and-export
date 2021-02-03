# Check preconditions
CONTAINER_NAME=${CONTAINER_NAME:-"mox_database"}
if ! [ -x "$(command -v docker)" ]; then
    echo "Unable to locate the 'docker' executable."
    exit 1
fi
if [ ! "$(docker ps -q -f name=${CONTAINER_NAME})" ]; then
    echo "Unable to locate a running mox database container: ${CONTAINER_NAME}"
    exit 1
fi

# Create backup
DATABASE_NAME=${DATABASE_NAME:-"mox"}
HOST_SNAPSHOT_DESTINATION=${HOST_SNAPSHOT_DESTINATION:-"/opt/docker/os2mo/database_snapshot/os2mo_database.sql"}
DOCKER_SNAPSHOT_DESTINATION=${DOCKER_SNAPSHOT_DESTINATION:-"/database_snapshot/os2mo_database.sql"}
echo "Snapshotting ${DATABASE_NAME}"
docker exec -t ${CONTAINER_NAME} \
    su --shell /bin/bash \
       --command "pg_dump --data-only ${DATABASE_NAME} -f ${DOCKER_SNAPSHOT_DESTINATION}" \
       postgres
EXIT_CODE=$?
if [ ${EXIT_CODE} -ne 0 ]; then
    echo "Unable to snapshot database"
    exit 1
fi
chmod 755 ${HOST_SNAPSHOT_DESTINATION}

# Create confdb
CONFDB_DATABASE_NAME=${CONFDB_DATABASE_NAME:-"mora"}
CONFDB_HOST_SNAPSHOT_DESTINATION=${CONFDB_HOST_SNAPSHOT_DESTINATION:-"/opt/docker/os2mo/database_snapshot/confdb.sql"}
CONFDB_DOCKER_SNAPSHOT_DESTINATION=${CONFDB_DOCKER_SNAPSHOT_DESTINATION:-"/database_snapshot/confdb.sql"}
echo "Snapshotting ${CONFDB_DATABASE_NAME}"
docker exec -t ${CONTAINER_NAME} \
    su --shell /bin/bash \
       --command "pg_dump --data-only --inserts ${CONFDB_DATABASE_NAME} -f ${CONFDB_DOCKER_SNAPSHOT_DESTINATION}" \
       postgres
EXIT_CODE=$?
if [ ${EXIT_CODE} -ne 0 ]; then
    echo "Unable to snapshot database"
    exit 1
fi
chmod 755 ${CONFDB_HOST_SNAPSHOT_DESTINATION}

# Create sessions
SESSIONS_DATABASE_NAME=${SESSIONS_DATABASE_NAME:-"sessions"}
SESSIONS_HOST_SNAPSHOT_DESTINATION=${SESSIONS_HOST_SNAPSHOT_DESTINATION:-"/opt/docker/os2mo/database_snapshot/sessions.sql"}
SESSIONS_DOCKER_SNAPSHOT_DESTINATION=${SESSIONS_DOCKER_SNAPSHOT_DESTINATION:-"/database_snapshot/sessions.sql"}
echo "Snapshotting ${SESSIONS_DATABASE_NAME}"
docker exec -t ${CONTAINER_NAME} \
    su --shell /bin/bash \
       --command "pg_dump --data-only --inserts ${SESSIONS_DATABASE_NAME} -f ${SESSIONS_DOCKER_SNAPSHOT_DESTINATION}" \
       postgres
EXIT_CODE=$?
if [ ${EXIT_CODE} -ne 0 ]; then
    echo "Unable to snapshot database"
    exit 1
fi
chmod 755 ${SESSIONS_HOST_SNAPSHOT_DESTINATION}

