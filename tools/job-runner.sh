#!/bin/bash
set +x
export DIPEXAR=${DIPEXAR:=$(cd $(dirname $0); pwd )/..}
export CUSTOMER_SETTINGS=${CUSTOMER_SETTINGS:=${DIPEXAR}/settings/settings.json}
export SETTINGS_FILE=$(basename ${CUSTOMER_SETTINGS})
export BACKUP_MAX_SECONDS_AGE=${BACKUP_MAX_SECONDS_AGE:=60}
export VENV=${VENV:=${DIPEXAR}/venv}
export IMPORTS_OK=false
export EXPORTS_OK=false
export REPORTS_OK=false
export BACKUP_OK=true
export LC_ALL="C.UTF-8"

source ${DIPEXAR}/tools/prefixed_settings.sh

cd ${DIPEXAR}
export PYTHONPATH=$PWD:$PYTHONPATH

# FIXME: remove cache ad pickle files
# Robert disables/moves them in later ad
# maybe he also takes care of the apos ones
rm *.p 2>/dev/null || :

# some logfiles can be truncated after backup as a primitive log rotation
# they should be appended to BACK_UP_AND_TRUNCATE
declare -a BACK_UP_AND_TRUNCATE=(
	${DIPEXAR}/mo_integrations.log
)

# files that need to be backed up BEFORE running the jobs
# should be appended to BACK_UP_BEFORE_JOBS NOW - they can't
# be added inside the job functions
declare -a BACK_UP_BEFORE_JOBS=(
    ${SNAPSHOT_LORA}
    $(readlink ${CUSTOMER_SETTINGS})
    $(
        SETTING_PREFIX="mox_stsorgsync" source ${DIPEXAR}/tools/prefixed_settings.sh
        # backup mox_stsorgsync config only if file exists
        [ -f "${MOX_MO_CONFIG}" ] && echo ${MOX_MO_CONFIG}
    )
    $(
        SETTING_PREFIX="integrations.SD_Lon.import" source ${DIPEXAR}/tools/prefixed_settings.sh
        # backup run_db only if file exists - it will not exist on non-SD customers
        echo ${run_db}
    )
)

# files that need to be backed up AFTER running the jobs
# should be appended to BACK_UP_AFTER_JOBS
declare -a BACK_UP_AFTER_JOBS=(
    ${CRON_LOG_FILE}
    # 2 files only exists at SD customers running changed at/cpr_uuid
    # always take them if they are there
    $([ -f "${DIPEXAR}/cpr_mo_ad_map.csv" ] && echo "${DIPEXAR}/cpr_mo_ad_map.csv")
    $([ -f "${DIPEXAR}/settings/cpr_uuid_map.csv" ] && echo "${DIPEXAR}/settings/cpr_uuid_map.csv")
)

show_git_commit(){
    echo
    echo CRON_GIT_COMMIT=$(git show -s --format=%H)
    echo
}

imports_mox_db_clear(){
    set -e
    echo running imports_mox_db_clear
    ${VENV}/bin/python3 tools/clear_mox_tables.py
}

imports_test_ad_connectivity(){
    set -e
    echo running imports_test_ad_connectivity
    ${VENV}/bin/python3 integrations/ad_integration/test_connectivity.py  --test-read-settings
}

imports_sd_fix_departments(){
    set -e
    echo running imports_sd_fix_departments
    SD_FIX_LOG="${SD_FIX_LOG:=/tmp/sd_fix_departments.log}"
    ${VENV}/bin/python3 integrations/SD_Lon/sd_fix_departments.py > ${SD_FIX_LOG}
}

imports_sd_changed_at(){
    set -e
    echo running imports_sd_changed_at
    BACK_UP_AFTER_JOBS+=(
        ${DIPEXAR}/cpr_mo_ad_map.csv
        ${DIPEXAR}/settings/cpr_uuid_map.csv
    )
    ${VENV}/bin/python3 integrations/SD_Lon/sd_changed_at.py
}

imports_opus_diff_import(){
    set -e
    echo running opus_diff_import
    BACK_UP_AFTER_JOBS+=(
        ${DIPEXAR}/cpr_mo_ad_map.csv
        ${DIPEXAR}/settings/cpr_uuid_map.csv
    )
    ${VENV}/bin/python3 integrations/opus/opus_diff_import.py
}

imports_ad_sync(){
    set -e
    echo running imports_ad_sync
    # remove ad cache files for now - they will be disabled later
    ${VENV}/bin/python3  integrations/ad_integration/ad_sync.py
}

imports_ballerup_apos(){
    set -e
    echo running imports_ballerup_apos
    ${VENV}/bin/python3 integrations/ballerup/ballerup.py
}

imports_ballerup_udvalg(){
    set -e
    BACK_UP_AND_TRUNCATE+=(
        "${DIPEXAR}/udvalg.log"
    )
    echo running imports_ballerup_udvalg
    ${VENV}/bin/python3 integrations/ballerup/udvalg_import.py
}

exports_mox_rollekatalog(){
    set -e
    echo running exports_mox_rollekatalog
    if [ -z "${MOX_ROLLE_COMPOSE_YML}" ]; then
        echo ERROR: MOX_ROLLE_COMPOSE_YML not set in configuration, aborting
        echo
        return 2
    fi
    docker-compose -f "${MOX_ROLLE_COMPOSE_YML}" up
}

exports_mox_stsorgsync(){
    set -e
    BACK_UP_AND_TRUNCATE+=($(
        SETTING_PREFIX="mox_stsorgsync" source ${DIPEXAR}/tools/prefixed_settings.sh
        echo ${LOGFILE}
    ))
    echo running exports_mox_stsorgsync
    (
        # get VENV, MOX_MO_CONFIG and LOGFILE
        SETTING_PREFIX="mox_stsorgsync" source ${DIPEXAR}/tools/prefixed_settings.sh
        ${VENV}/bin/python3 -m mox_stsorgsync >> ${LOGFILE} 2>&1 || exit 2
        echo "Last 50 lines from mox_stsorgsyncs log :"
        tail  -n 50  ${LOGFILE}
        echo "last 10 Errors from OS2sync"
        grep ERROR /var/log/os2sync/service.log | tail -n 10
    )
}

exports_os2mo_phonebook(){
    set -e
    :
}

exports_cpr_uuid(){
    set -e
    echo running exports_cpr_uuid
    ${VENV}/bin/python3 exporters/cpr_uuid.py
}

reports_sd_db_overview(){
    set -e
    echo running reports_sd_db_overview
    ${VENV}/bin/python3 integrations/SD_Lon/db_overview.py
}

reports_opus_db_overview(){
    set -e
    echo running reports_opus_db_overview
    ${VENV}/bin/python3 integrations/opus/db_overview.py
}


exports_queries_ballerup(){
    set -e
    echo appending ballerup exports logfile to BACK_UP_AND_TRUNCATE
    BACK_UP_AND_TRUNCATE+=($(
        SETTING_PREFIX="exporters.ballerup" source ${DIPEXAR}/tools/prefixed_settings.sh
        echo ${WORK_DIR}/export.log
    ))
    echo running exports_queries_ballerup
    (
	set -x
        # get OUT_DIR and EXPORTS_DIR
        SETTING_PREFIX="exporters.ballerup" source ${DIPEXAR}/tools/prefixed_settings.sh
        [ -z "${EXPORTS_DIR}" ] && echo "EXPORTS_DIR not spec'ed for exports_queries_ballerup" && exit 1
        [ -z "${WORK_DIR}" ] && echo "WORK_DIR not spec'ed for exports_queries_ballerup" && exit 1
        [ -d "${WORK_DIR}" ] || mkdir "${WORK_DIR}"
        cd "${WORK_DIR}"
        ${VENV}/bin/python3 ${DIPEXAR}/exporters/ballerup.py > ${WORK_DIR}/export.log 2>&1
        mv "${WORK_DIR}"/*.csv "${EXPORTS_DIR}"
    )
}

exports_test(){
    set -e
    :
}

# imports are typically interdependent: -e
imports(){
    [ "${BACKUP_OK}" == "false" ] \
        && echo ERROR: backup is in error - skipping imports \
        && return 1 # imports depend on backup

    if [ "${RUN_MOX_DB_CLEAR}" == "true" ]; then
        imports_mox_db_clear || return 2
    fi

    if [ "${RUN_CHECK_AD_CONNECTIVITY}" == "true" ]; then
        imports_test_ad_connectivity || return 2
    fi

    if [ "${RUN_SD_FIX_DEPARTMENTS}" == "true" ]; then
        imports_sd_fix_departments || return 2
    fi

    if [ "${RUN_SD_CHANGED_AT}" == "true" ]; then
        imports_sd_changed_at || return 2
    fi

    if [ "${RUN_OPUS_DIFF_IMPORT}" == "true" ]; then
        imports_opus_diff_import || return 2
    fi

    if [ "${RUN_AD_SYNC}" == "true" ]; then
        imports_ad_sync || return 2
    fi

    if [ "${RUN_BALLERUP_APOS}" == "true" ]; then
        imports_ballerup_apos || return 2
    fi

    if [ "${RUN_BALLERUP_UDVALG}" == "true" ]; then
        imports_ballerup_udvalg || return 2
    fi
}

# exports may also be interdependent: -e
exports(){
    [ "${IMPORTS_OK}" == "false" ] \
        && echo ERROR: imports are in error - skipping exports \
        && return 1 # exports depend on imports

    if [ "${RUN_MOX_ROLLE}" == "true" ]; then
        exports_mox_rollekatalog || return 2
    fi

    if [ "${RUN_MOX_STS_ORGSYNC}" == "true" ]; then
        exports_mox_stsorgsync || return 2
    fi

    if [ "${RUN_QUERIES_BALLERUP}" == "true" ]; then
        exports_queries_ballerup || return 2
    fi

    if [ "${RUN_EXPORTS_OS2MO_PHONEBOOK}" == "true" ]; then
        exports_os2mo_phonebook || return 2
    fi

    if [ "${RUN_CPR_UUID}" == "true" ]; then
        # this particular report is not allowed to fail
        exports_cpr_uuid || return 2
    fi

    if [ "${RUN_EXPORTS_TEST}" == "true" ]; then
        exports_test || return 2
    fi
}

# reports are typically not interdependent
reports(){
    #set -x # debug log
    [ "${IMPORTS_OK}" == "false" ] \
        && echo ERROR: imports are in error - skipping reports \
        && return 1 # reports depend on imports

    if [ "${RUN_SD_DB_OVERVIEW}" == "true" ]; then
        reports_sd_db_overview || echo "error in reports_sd_db_overview - continuing"
    fi
    
    if [ "${RUN_OPUS_DB_OVERVIEW}" == "true" ]; then
        reports_opus_db_overview || echo "error in reports_opus_db_overview - continuing"
    fi

}

pre_truncate_logfiles(){
    # logfiles are truncated before each run as 
    [ -f "udvalg.log" ] && truncate -s 0 "udvalg.log" 
}

pre_backup(){
    for f in ${BACK_UP_BEFORE_JOBS[@]}
    do
        # try to append to tar file and report if not found
	tar -rf $BUPFILE "${f}" > /dev/null 2>&1 || BACKUP_OK=false 
    done
    declare -i age=$(stat -c%Y ${BUPFILE})-$(stat -c%Y ${SNAPSHOT_LORA})
    if [[ ${age} -gt ${BACKUP_MAX_SECONDS_AGE} ]]; then
        BACKUP_OK=false 
	echo "ERROR database snapshot is more than ${BACKUP_MAX_SECONDS_AGE} seconds old: $age"
    fi
}

post_backup(){
    for f in ${BACK_UP_AFTER_JOBS[@]} ${BACK_UP_AND_TRUNCATE[@]}
    do
	tar -rf $BUPFILE "${f}" > /dev/null 2>&1 || BACKUP_OK=false 
    done

    echo
    echo listing preliminary backup archive
    echo ${BUPFILE}.gz
    tar -tvf ${BUPFILE}
    gzip  ${BUPFILE}

    echo truncating backed up logfiles
    truncate -s 0 ${BACK_UP_AND_TRUNCATE[@]}

    echo
    BACKUP_SAVE_DAYS=${BACKUP_SAVE_DAYS:=90}
    echo deleting backups older than "${BACKUP_SAVE_DAYS}" days
    bupsave=${CRON_BACKUP}/$(date +%Y-%m-%d-%H-%M-%S -d "-${BACKUP_SAVE_DAYS} days")-cron-backup.tar.gz
    for oldbup in ${CRON_BACKUP}/????-??-??-??-??-??-cron-backup.tar.gz
    do
        [ "${oldbup}" \< "${bupsave}" ] && (
            rm -v ${oldbup}
        )
    done
}

if [ "$#" == "0" ]; then
    (
    if [ ! -d "${VENV}" ]; then
        echo "FATAL: python env not found"
        exit 2 # error
    fi

    if [ ! -n "${SVC_USER}" ]; then
        echo "WARNING: Service user not specified"
    fi

    if [ ! -n "${SVC_KEYTAB}" ]; then
        echo "WARNING: Service keytab not specified"
    fi

    if [ -n "${SVC_KEYTAB}" -a ! -f "${SVC_KEYTAB}" ]; then
        echo "FATAL: Service keytab not found"
        exit 2
    fi

    if [ ! -n "${CRON_LOG_FILE}" ]; then
        echo "FATAL: Cron log file not specified"
        exit 2
    fi

    if [ ! -n "${CRON_BACKUP}" ]; then
        echo "FATAL: Backup directory not specified"
        exit 2
    fi

    if [ ! -d "${CRON_BACKUP}" ]; then
        echo "FATAL: Backup directory non existing"
        exit 2
    fi

    if [ ! -f "${SNAPSHOT_LORA}" ]; then
        echo "FATAL: Database snapshot does not exist"
        exit 2
    fi

    if [ -n "${SVC_USER}" -a -n "${SVC_KEYTAB}" ]; then
        kinit ${SVC_USER} -k -t ${SVC_KEYTAB}
    else
        echo WARNING: not able to refresh kerberos auth
    fi

    export BUPFILE=${CRON_BACKUP}/$(date +%Y-%m-%d-%H-%M-%S)-cron-backup.tar

    show_git_commit
    pre_backup
    imports && IMPORTS_OK=true
    exports && EXPORTS_OK=true
    reports && REPORTS_OK=true
    echo IMPORTS_OK=${IMPORTS_OK}
    echo EXPORTS_OK=${EXPORTS_OK}
    echo REPORTS_OK=${REPORTS_OK}
    echo BACKUP_OK=${BACKUP_OK}
    show_git_commit
    post_backup
    ) 2>&1 | tee ${CRON_LOG_FILE}

elif [ -n "$(grep $1\(\) $0)" ]; then
    echo running single job function
    $1
fi
