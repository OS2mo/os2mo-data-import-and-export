#!/bin/bash
export DIPEXAR=${DIPEXAR:=$(cd $(dirname $0); pwd )/..}
export CUSTOMER_SETTINGS=${CUSTOMER_SETTINGS:=${DIPEXAR}/settings/settings.json}
export SETTINGS_FILE=$(basename ${CUSTOMER_SETTINGS})
export VENV=${VENV:=${DIPEXAR}/venv}
export IMPORTS_OK=false
export EXPORTS_OK=false
export REPORTS_OK=false
export LC_ALL="C.UTF-8"

source ${DIPEXAR}/tools/prefixed_settings.sh

#set -x

cd ${DIPEXAR}
export PYTHONPATH=$PWD:$PYTHONPATH

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

kinit ${SVC_USER} -k -t ${SVC_KEYTAB}

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
    ${VENV}/bin/python3 integrations/SD_Lon/sd_fix_departments.py >/dev/null
}

imports_sd_changed_at(){
    set -e
    echo running imports_sd_changed_at
    ${VENV}/bin/python3 integrations/SD_Lon/sd_changed_at.py
}

reports_sd_db_overview(){
    set -e
    echo running reports_sd_db_overview
    ${VENV}/bin/python3 integrations/SD_Lon/db_overview.py
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
}

if [ "$#" == "0" ]; then
    show_git_commit
    imports && IMPORTS_OK=true
    exports && EXPORTS_OK=true
    reports && REPORTS_OK=true
    show_git_commit
    echo IMPORTS_OK=${IMPORTS_OK}
    echo EXPORTS_OK=${EXPORTS_OK}
    echo REPORTS_OK=${REPORTS_OK}
fi
