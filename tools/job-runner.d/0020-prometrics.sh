#!/bin/bash
prometrics-ts(){
    declare -i NOW=$(date +%s)
    then=$1
    then=${then:=0}
    declare -i now=$(($NOW-$then))
    echo $now
}

prometrics-job-end(){
[ -z "${CRON_LOG_PROM_API}" ] && return 0
cat <<EOF | curl -m 2 -sS --data-binary @- "${CRON_LOG_PROM_API}"
# TYPE ${JOBNAME}_duration_seconds gauge
# HELP ${JOBNAME}_duration_seconds Duration of batch job
${JOBNAME}_duration_seconds ${JOBTIME}
EOF
}

prometrics-job-success(){
[ -z "${CRON_LOG_PROM_API}" ] && return 0
cat <<EOF | curl -m 2 -sS --data-binary @- "${CRON_LOG_PROM_API}"
# TYPE ${JOBNAME}_last_success gauge
# HELP ${JOBNAME}_last_success Unixtime job-runner last succeeded
${JOBNAME}_last_success ${JOBTIME}
EOF
}
