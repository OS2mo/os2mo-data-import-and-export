#!/bin/bash

# Usage:
# 1. Create the folder /opt/cron/
# 2. Add the script to root's crontab: "05 06 * docker exec -t dipex /code/tools/docker-cron.sh"
# 3. Verify

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Database snapshot
#------------------
curl --header "Content-Type: application/json" --data '{"query":"mutation backup {createBackup}"}' http://backup_service:8000

# Run script between pre and post-hooks
#-----------
${DIR}/job-runner.sh
