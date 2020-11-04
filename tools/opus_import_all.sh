#/bin/bash

#
# get settings and some functions from job-runner.sh
#

run_db=$(SETTING_PREFIX="integrations.opus.import" . tools/prefixed_settings.sh; echo $run_db)
if [ -z "${run_db}" ]; then
    echo run_db skal i backuppen, og den er ikke defineret
    exit 1
fi

xml_path=$(SETTING_PREFIX="integrations.opus.import" . tools/prefixed_settings.sh; echo $xml_path)
if [ -z "${xml_path}" ]; then
    echo xml_path for opus files not set in settings file
    exit 1
fi

municipality_name=$(SETTING_PREFIX="municipality" . tools/prefixed_settings.sh; echo $name)
municipality_name=${municipality_name,,}
municipality_name=${municipality_name% *kommune}
if [ -z "${municipality_name}" ]; then
    echo municipality name not set in settings file
    exit 1
fi

#
# for all xml files run a standard diff import - some may do nothing - and take a backup
#

. tools/job-runner.sh
set +e
STOP=false
for xml in ${xml_path}/* ; do
    set -x
    if [ -f "stop" ]; then
        echo found file \'stop\' - stopping
        exit 0
    fi
    export BUPFILE="${CRON_BACKUP}/$(date +%Y-%m-%d-%H-%M-%S-%N)-opus-import-all.tar"
    echo using $BUPFILE
    declare -a BACK_UP_AND_TRUNCATE=(${DIPEXAR}/mo_integrations.log)
    declare -a BACK_UP_BEFORE_JOBS=(
        ${SNAPSHOT_LORA} 
        $(readlink ${CUSTOMER_SETTINGS}) 
        ${run_db}
    )
    declare -a BACK_UP_AFTER_JOBS=(${CRON_LOG_FILE})
    sudo docker exec -t mox_database su --shell /bin/bash --command "pg_dump --data-only mox -f /database_snapshot/os2mo_database.sql" postgres
    pre_backup || exit 1
    ${VENV}/bin/python3 integrations/opus/opus_diff_import.py
    # due to somewhat arbitrary return value from script above
    [ $(wc -l ${DIPEXAR}/mo_integrations.log | cut -f1 -d" ") -gt 20 ] || STOP=true
    post_backup 
    reports_opus_db_overview
    [ "$STOP" = "true" ] && exit 0
done
