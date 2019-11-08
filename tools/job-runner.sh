#!/bin/bash
set +x
export DIPEXAR=${DIPEXAR:=$(cd $(dirname $0); pwd )/..}
export CUSTOMER_SETTINGS=${CUSTOMER_SETTINGS:=${DIPEXAR}/settings/settings.json}
export SETTINGS_FILE=$(basename ${CUSTOMER_SETTINGS})
export VENV=${VENV:=${DIPEXAR}/venv}
export IMPORTS_OK=false
export EXPORTS_OK=false
export REPORTS_OK=false
export LC_ALL="C.UTF-8"

source ${DIPEXAR}/tools/prefixed_settings.sh

cd ${DIPEXAR}
export PYTHONPATH=$PWD:$PYTHONPATH


show_git_commit(){
    echo
    echo CRON_GIT_COMMIT=$(git show -s --format=%H)
}

imports_test_ad_connectivity(){
    set -e
    echo running imports_test_ad_connectivity
    ${VENV}/bin/python3 integrations/ad_integration/test_connectivity.py
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
    ${VENV}/bin/python3 integrations/SD_Lon/sd_changed_at.py
}

exports_mox_stsorgsync(){
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

reports_sd_db_overview(){
    set -e
    echo running reports_sd_db_overview
    ${VENV}/bin/python3 integrations/SD_Lon/db_overview.py
}

reports_cpr_uuid(){
    set -e
    echo running reports_cpr_uuid
    ${VENV}/bin/python3 exporters/cpr_uuid.py
}

# imports are typically interdependent: -e
imports(){
    if [ "${RUN_CHECK_AD_CONNECTIVITY}" == "true" ]; then
        imports_test_ad_connectivity || return 2
    fi

    if [ "${RUN_SD_FIX_DEPARTMENTS}" == "true" ]; then
        imports_sd_fix_departments || return 2
    fi

    if [ "${RUN_SD_CHANGED_AT}" == "true" ]; then
        imports_sd_changed_at || return 2
    fi
}

# exports may also be interdependent: -e
exports(){
    [ "${IMPORTS_OK}" == "false" ] \
        && echo imports are in error - skipping exports \
        && return 1 # exports depend on imports

    if [ "${RUN_MOX_STS_ORGSYNC}" == "true" ]; then
        exports_mox_stsorgsync || return 2
    fi
}

# reports are typically not interdependent
reports(){
    #set -x # debug log
    [ "${IMPORTS_OK}" == "false" ] \
        && echo imports are in error - skipping reports \
        && return 1 # reports depend on imports

    if [ "${RUN_SD_DB_OVERVIEW}" == "true" ]; then
        reports_sd_db_overview || echo "error in reports_sd_db_overview - continuing"
    fi

    if [ "${RUN_CPR_UUID}" == "true" ]; then
        # this particular report is not allowed to fail
        reports_cpr_uuid || return 2
    fi
}

post_backup(){
    # some files are not parameterised yet, others are.
    # primitive backup, I know - but until something better turns up...
    STS_ORG_CONFIG=$(
        SETTING_PREFIX="mox_stsorgsync" source ${DIPEXAR}/tools/prefixed_settings.sh
        echo ${MOX_MO_CONFIG}
    )
    SD_IMPORT_RUN_DB=$(
        SETTING_PREFIX="integrations.SD_Lon.import" source ${DIPEXAR}/tools/prefixed_settings.sh
        echo ${run_db}
    )
    bupfile="${CRON_BACKUP}/$(date +%Y-%m-%d-%H-%M-%S)-cron-backup.tar.gz"
    tar -zcf $bupfile\
        /opt/magenta/snapshots/os2mo_database.sql \
        ${DIPEXAR}/cpr_mo_ad_map.csv \
        ${DIPEXAR}//settings/cpr_uuid_map.csv \
        ${SD_IMPORT_RUN_DB} \
        $(readlink ${CUSTOMER_SETTINGS}) \
        ${CRON_LOG_FILE} \
        ${STS_ORG_CONFIG} \
        > /dev/null 2>&1

    echo
    echo listing preliminary backup archive
    echo ${bupfile}
    tar -tvf ${bupfile}
}

if [ "$#" == "0" ]; then
    (
    if [ ! -d "${VENV}" ]; then
        echo "python env not found"
        exit 2 # error
    fi

    if [ ! -n "${SVC_USER}" ]; then
        echo "Service user not specified"
        exit 2
    fi

    if [ ! -n "${SVC_KEYTAB}" ]; then
        echo "Service keytab not specified"
        exit 2
    fi

    if [ ! -f "${SVC_KEYTAB}" ]; then
        echo "Service keytab not found"
        exit 2
    fi

    if [ ! -n "${CRON_LOG_FILE}" ]; then
        echo "Cron log file not specified"
        exit 2
    fi

    if [ ! -n "${CRON_BACKUP}" ]; then
        echo "Backup directory not specified"
        exit 2
    fi

    if [ ! -d "${CRON_BACKUP}" ]; then
        echo "Backup directory non existing"
        exit 2
    fi

    kinit ${SVC_USER} -k -t ${SVC_KEYTAB}

    show_git_commit
    imports && IMPORTS_OK=true
    exports && EXPORTS_OK=true
    reports && REPORTS_OK=true
    post_backup
    show_git_commit
    echo IMPORTS_OK=${IMPORTS_OK}
    echo EXPORTS_OK=${EXPORTS_OK}
    echo REPORTS_OK=${REPORTS_OK}
    ) 2>&1 | tee ${CRON_LOG_FILE}
fi
