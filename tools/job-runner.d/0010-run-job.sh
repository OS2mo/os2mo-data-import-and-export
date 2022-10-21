# denne fil skal sources af job-runner.sh
run-job(){
    local JOB=$1
    prometrics-job-start ${JOB}
    $JOB
    JOB_STATUS=$?
    prometrics-job-end ${JOB} ${JOB_STATUS}
    return $JOB_STATUS
}
