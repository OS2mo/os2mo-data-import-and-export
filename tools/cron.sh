#!/bin/bash

# Usage:
# 1. Create the folder /opt/cron/
# 2. Copy this script to /opt/cron/os2mo-data.sh
# 3. Modify the below variables according to need
# 4. Add the script to root's crontab: "05 06 * /opt/cron/os2mo-data.sh"
# 5. Verify

# Configuration
#--------------
# Absolute path to the job-runner.sh script
SCRIPT=/home/viborg.local/svc_os2mo/CRON/os2mo-data-import-and-export/tools/job-runner.sh
# SCRIPT=/home/emil/projects/mo/os2mo-data-import-and-export/tools/job-runner.sh

# Unix service account to run job-runner.sh under
RUNAS=svc_os2mo
# RUNAS=emil

# Installation type for backup (docker, legacy or none)
INSTALLATION_TYPE=docker

# Email configuration, a valid login to Googles SMTP
SNAIL_USERNAME=slareport@magenta-aps.dk
SNAIL_PASSWORD=Password1

# Email recipients
SNAIL_RECIPIENTS=os2mo-ops@magenta-aps.dk
# SNAIL_RECIPIENTS=emil@magenta.dk

# Source helper functions
#------------------------
function generate_mail() {
    EXIT_CODE=$1
    SCRIPT_OUTPUT=$2
    echo "This is an OPS notification email"
    echo ""
    echo "Alert received from $(hostname)"
    echo ""
    echo "Script ${SCRIPT} exited with code ${EXIT_CODE}"
    echo ""
    echo "==== STDOUT ===="
    echo "${SCRIPT_OUTPUT}"
}

function send_email() {
    MAIL_SUBJECT=$1
    MAIL_BODY=$2

	echo "Sending email"
    echo "${EMAIL_SUBJECT}"
    echo ""
    echo "${EMAIL_BODY}"
    echo ""
	echo "${MAIL_BODY}" | s-nail -v -s "${MAIL_SUBJECT}" -S smtp-use-starttls -S ssl-verify=ignore -S smtp-auth=login -S smtp=smtp://smtp.gmail.com -S from=${SNAIL_USERNAME} -S smtp-auth-user=${SNAIL_USERNAME} -S smtp-auth-password=${SNAIL_PASSWORD} ${SNAIL_RECIPIENTS}
	echo "Send!"
}

# Preconditions
#--------------
# Check if the script exists
if [ ! -f ${SCRIPT} ]; then
    echo "Unable to locate script in specified path: ${SCRIPT}"
    exit 1
fi

# Check if the user exists
if ! id "${RUNAS}" >/dev/null 2>&1; then
	echo "Unable to locate the specified runas user: ${RUNAS}"
	exit 1
fi

# Check for necessary tools
if ! [ -x "$(command -v s-nail)" ]; then
    echo "Unable to locate the 's-nail' executable."
    echo "Try: sudo apt-get install s-nail"
    exit 1
fi

# Database snapshot
#------------------
if [ "${INSTALLATION_TYPE}" == "docker" ]; then
    # Check preconditions
    # CONTAINER_NAME="mox_database"
    CONTAINER_NAME="os2mo_mox-db_1"
    DATABASE_NAME="mox"
    HOST_SNAPSHOT_DESTINATION="/opt/docker/os2mo/database_snapshot/os2mo_database.sql"
    # DOCKER_SNAPSHOT_DESTINATION="/database_snapshot/os2mo_database.sql"
    DOCKER_SNAPSHOT_DESTINATION="/tmp/os2mo_database.sql"
    if ! [ -x "$(command -v docker)" ]; then
        echo "Unable to locate the 'docker' executable."
        exit 1
    fi
    if [ ! "$(docker ps -q -f name=${CONTAINER_NAME})" ]; then
        echo "Unable to locate a running mox database container: ${CONTAINER_NAME}"
        exit 1
    fi
    # Create backup
    docker exec -t ${CONTAINER_NAME} \
        su --shell /bin/bash \
           --command "pg_dump --data-only ${DATABASE_NAME} -f ${DOCKER_SNAPSHOT_DESTINATION}" \
           postgres
    EXIT_CODE=$?
    if [ ${EXIT_CODE} -ne 0 ]; then
        echo "Unable to snapshot database"
        exit 1
    fi
    # docker cp ${CONTAINER_NAME}:${DOCKER_SNAPSHOT_DESTINATION} ${HOST_SNAPSHOT_DESTINATION}
    chmod 755 ${HOST_SNAPSHOT_DESTINATION}
elif [ "${INSTALLATION_TYPE}" == "legacy" ]; then
    # Check preconditions
    HOST_SNAPSHOT_DESTINATION="/opt/magenta/snapshots/os2mo_database.sql"
    if ! [ -x "$(command -v pg_dump)" ]; then
        echo "Unable to locate the 'pg_dump' executable."
        exit 1
    fi
    # Ensure the folder exists
    mkdir -p $(dirname "${HOST_SNAPSHOT_DESTINATION}")
    # Create backup
    su --shell /bin/bash \
        --command "pg_dump --data-only ${DATABASE_NAME} -f ${HOST_SNAPSHOT_DESTINATION}"
        postgres
    EXIT_CODE=$?
    if [ ${EXIT_CODE} -ne 0 ]; then
        echo "Unable to snapshot database"
        exit 1
    fi
elif [ "${INSTALLATION_TYPE}" == "none" ]; then
	echo "WARNING: No snapshotting configured"
else
	echo "Unknown installation type: ${INSTALLATION_TYPE}"
	exit 1
fi

# Run script
#-----------
export CRON_LOG_FILE=$(mktemp)
SCRIPT_OUTPUT=$(su --preserve-environment --shell /bin/bash --command "${SCRIPT}" ${RUNAS})
EXIT_CODE=$?

EMAIL_SUBJECT="[OS2MO-OPS] OS2MO integration runner"
EMAIL_BODY=$(generate_mail "$EXIT_CODE" "$SCRIPT_OUTPUT")

send_email "${EMAIL_SUBJECT}" "${EMAIL_BODY}"

if [ "${EXIT_CODE}" -eq 0 ]; then
	echo "Script ran succesfully"
    exit 0
else
	echo "Script has failed to execute"
	exit ${EXIT_CODE}
fi
