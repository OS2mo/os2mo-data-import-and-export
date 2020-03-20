run-job-log (){
    [ -z "${CRON_LOG_JSON_SINK}" ] && return 0
    # write a line of json to the log
    declare -A KWS=([time]=$(date +"%Y-%m-%dT%H:%M:%S %z"))
    declare -a order=("time")
     
    while read KEY VAL
    do
        [ -z "$KEY" ] && continue  
        order+=("$KEY") 
        KWS["${KEY}"]="$VAL"
    done < <(echo "$*" | tr "!" "\n")

    (
        echo '{'
	COMMA=""
        for K in "${order[@]}"; do 
            echo $COMMA
            echo \"$K\": \""${KWS[$K]}"\"
            COMMA=","
	done
        echo '}'
    ) | jq -c . | tee -a ${CRON_LOG_JSON_SINK} || echo could not write to ${CRON_LOG_JSON_SINK}
}

run-job(){
    JOB=$1
    # [ ! "$JOB" = "imports" ] && JOB=true # testing
    run-job-log ! job $1 ! job-status starting ! $COMMENT

    $JOB

    if [ "$?" = "0" ] ; then
        run-job-log ! job $1 ! job-status success ! $COMMENT
        return 0
    else
        run-job-log ! job $1 ! job-status failed  ! $COMMENT 
        return 1
    fi
}

