# denne fil skal sources af job-runner.sh
run_job_date=$(date +"%Y-%m-%d")
run_job_batch_number=$(($(find "${CRON_BACKUP}" -name ${run_job_date}'*' | wc -l) + 1 ))
run_job_log_json=${CRON_LOG_JSON:=/dev/null}

run-job-log (){
    LOGLINE="$*"
    [ -n "${BATCH_COMMENT}" ] && LOGLINE="$LOGLINE ! batch-comment $BATCH_COMMENT !"
    [ -z "${CRON_LOG_JSON_SINK}" ] && return 0
    # write a line of json to the log
    declare -A KWS=(
        [time]="$(date +"%Y-%m-%dT%H:%M:%S %z")"
        [date]="${run_job_date}"
        [batch]="${run_job_batch_number}"
    )
    declare -a order=("time" "date" "batch")
     
    while read KEY VAL
    do
        [ -z "$KEY" ] && continue  
        order+=("$KEY") 
        KWS["${KEY}"]="$VAL"
    done < <(echo "${LOGLINE}" | tr "!" "\n")

    (
        echo '{'
	COMMA=""
        for K in "${order[@]}"; do 
            echo $COMMA
            echo \"$K\": \""${KWS[$K]}"\"
            COMMA=","
	done
        echo '}'
    ) | jq -c . | tee -a ${run_job_log_json} >> ${CRON_LOG_JSON_SINK} || echo could not write to ${CRON_LOG_JSON_SINK}
}

run-job(){
    local JOB=$1
    run-job-log ! job $1 ! job-status starting !

    prometrics-job-start mo_${JOB}
    $JOB
    JOB_STATUS=$?
    prometrics-job-end mo_${JOB} ${JOB_STATUS}

    if [ "$JOB_STATUS" = "0" ] ; then
        run-job-log ! job $JOB ! job-status success !
    else
        run-job-log ! job $JOB ! job-status failed !
    fi
    return $JOB_STATUS
}
