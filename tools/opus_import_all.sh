#/bin/bash

#
# get settings and some functions from job-runner.sh
#

. tools/job-runner.sh
if [ "$EUID" -ne 0 -o "${JOB_RUNNER_MODE}" != "sourced" ]; then
    echo this script must be run as user root from the root of the os2mo-data-import-and-export folder
    exit 1
fi

if [ -z "${SVC_USER}" ]; then
    echo ${SETTING_PREFIX}.SVC_USER not set in settings file
    exit 1
fi
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

set +e
for xml in ${xml_path}/* ; do
    #set -x
    # root can be hard to kill - a stop file can do it
    # in the root of os2mo-data-import-and-export
    if [ -f "stop" ]; then
        echo found file \'stop\' - stopping
        exit 0
    fi
    export BUPFILE="${CRON_BACKUP}/$(date +%Y-%m-%d-%H-%M-%S-%N)-opus-import-all.tar"
    export municipality_name xml run_db
    echo using $BUPFILE
    salt-call os2mo.create_db_snapshot installation_type=docker
    su -p -c '(
        kinit ${SVC_USER} -k -t ${SVC_KEYTAB} || exit 1
        STOP=0
        #set -x
        declare -a BACK_UP_AND_TRUNCATE=(${DIPEXAR}/mo_integrations.log)
        declare -a BACK_UP_BEFORE_JOBS=(
            ${SNAPSHOT_LORA} 
            $(readlink ${CUSTOMER_SETTINGS}) 
            ${run_db}
        )
        declare -a BACK_UP_AFTER_JOBS=(${CRON_LOG_FILE})
        pre_backup || STOP=1
        ${VENV}/bin/python3 integrations/${municipality_name}/${municipality_name}.py --update
        reports_opus_db_overview
        # due to somewhat arbitrary return value from script above
        [ $(wc -l ${DIPEXAR}/mo_integrations.log | cut -f1 -d" ") -gt 20 ] || STOP=1
        post_backup 
        exit $STOP
    )' ${SVC_USER} > ${CRON_LOG_FILE} 2>&1
    if [ ! "$?" = "0" ]; then
        echo subshell returned bad apples
        exit 1
    fi
    reports_opus_db_overview
done

# report changedate per logfile
for i in ${CRON_BACKUP}/*opus-import-all*
do
    firstline=$(tar -xOf $i ${CRON_LOG_FILE#/}| head --quiet --lines=1 )
    echo ${firstline##*update}: $i
done
echo Critical Error betyder at jobbet har været startet efter en uafsluttet kørsel
echo I så tilfælde stopper programmet uden af lave noget
