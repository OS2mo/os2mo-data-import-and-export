#!/bin/bash
prometrics-job-start(){
[ -z "${CRON_LOG_PROM_API}" ] && return 0
cat <<EOF | curl -m 2 -sS --data-binary @- "${CRON_LOG_PROM_API}/$1/"
# TYPE mo_start_time gauge
# HELP mo_start_time Unixtime job-runner start time
mo_start_time $(date +%s)
EOF
}

prometrics-job-end(){
[ -z "${CRON_LOG_PROM_API}" ] && return 0
cat <<EOF | curl -m 2 -sS --data-binary @- "${CRON_LOG_PROM_API}/$1"
# TYPE mo_end_time gauge
# HELP mo_end_time Unixtime job-runner end time
mo_end_time $(date +%s)
# TYPE mo_rc gauge
# HELP mo_rc returncode of job-runner 
mo_rc $2
EOF
}

