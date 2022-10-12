#!/bin/bash
prometrics-job-start(){
    [ -z "${CRON_LOG_PROM_API}" ] && return 0
    cat <<EOF | curl -m 2 -sS --data-binary @- "${CRON_LOG_PROM_API}/$1/"
    # TYPE mo_start_time gauge
    # HELP mo_start_time Unixtime for job start time
    mo_start_time $(date +%s)
EOF
}

prometrics-job-end(){
    [ -z "${CRON_LOG_PROM_API}" ] && return 0
    cat <<EOF | curl -m 2 -sS --data-binary @- "${CRON_LOG_PROM_API}/$1"
    # TYPE mo_end_time gauge
    # HELP mo_end_time Unixtime for job end time
    mo_end_time $(date +%s)
    # TYPE mo_return_code gauge
    # HELP mo_return_code Return code of job
    mo_return_code $2
EOF
}

prometrics-git(){
    
    git_version=$(git describe --tags)
    
    [ -z "${CRON_LOG_PROM_API}" ] && return 0
    cat <<EOF | curl -m 2 -sS --data-binary @- "${CRON_LOG_PROM_API}/git/git_version/${git_version}"
    # TYPE git_info gauge
    # HELP git_info A metric with a timestamp to sort by, labeled by git_hash, branch and local_changes
    git_info $(date +%s)
EOF
}
