# denne fil skal sources af job-runner.sh
run-job(){
    local JOB=$1
    prometrics-job-start ${JOB}

    # Detect if we are running from cron
    if [ "$TERM" == "dumb" ]; then
        # Capture both stdout and stderr using "|&" (requires Bash 4+.)
        # Send stdout and stderr to systemd journal using the identifier given by the "-t" option.
        # Job output can be retrieved using e.g. "journalctl -t dipex:job-runner.sh:exports_actual_state_export", etc.
        $JOB |& systemd-cat -t "dipex:job-runner.sh:$JOB"
    else
        $JOB
    fi

    JOB_STATUS=$?
    prometrics-job-end ${JOB} ${JOB_STATUS}
    return $JOB_STATUS
}
