#!/bin/bash

export DIPEXAR=${DIPEXAR:=$(realpath -L $(dirname $(realpath -L "${BASH_SOURCE}"))/..)}
cd ${DIPEXAR}

# read the run-job script et al
source tools/job-runner.sh

prometrics-job-start "backup"

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
bash tools/data_dump/backup.sh ${CONTAINER_NAME} ${DATABASE_NAME} "/database_snapshot"
EXIT_CODE=$?

prometrics-job-end "backup" ${EXIT_CODE}
if [ ${EXIT_CODE} -ne 0 ]; then
    echo "Unable to snapshot database"
    exit 1
fi
chmod 755 ${HOST_SNAPSHOT_DESTINATION}
