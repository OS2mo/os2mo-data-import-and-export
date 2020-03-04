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

cd ${DIPEXAR}
source ${DIPEXAR}/tools/prefixed_settings.sh
cd ${DIPEXAR}

export PYTHONPATH=$PWD:$PYTHONPATH

rm tmp/*.p 2>/dev/null || :

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
    $(
        SETTING_PREFIX="integrations.opus.import" source ${DIPEXAR}/tools/prefixed_settings.sh
        # backup run_db only if file exists - it will not exist on non-OPUS customers
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
    echo "    CRON_GIT_COMMIT=$(git show -s --format=%H)"
    echo
}

imports_mox_db_clear(){
    set -e
    echo running imports_mox_db_clear
    ${VENV}/bin/python3 tools/clear_mox_tables.py
}

imports_test_ad_connectivity(){
    set -e
    BACK_UP_AND_TRUNCATE+=(
        "${DIPEXAR}/test_connectivity.log"
    )
    echo running imports_test_ad_connectivity
    ${VENV}/bin/python3 integrations/ad_integration/test_connectivity.py  --test-read-settings
}

imports_sd_fix_departments(){
    set -e
    BACK_UP_AND_TRUNCATE+=(
        "${DIPEXAR}/fix_sd_departments.log"
    )
    echo running imports_sd_fix_departments
    ${VENV}/bin/python3 integrations/SD_Lon/sd_fix_departments.py
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

imports_sd_update_primary(){
    BACK_UP_AND_TRUNCATE+=(
        "${DIPEXAR}/calculate_primary.log"
    )
    echo updating primary engagements
    ${VENV}/bin/python3 integrations/SD_Lon/calculate_primary.py --recalculate-all || (
        # denne fejl skal ikke stoppe afviklingen, da en afbrudt kørsel blot kan gentages
        echo FEJL i updating primary engagements, men kører videre
    )
}


imports_ad_sync(){
    set -e
    BACK_UP_AND_TRUNCATE+=(
        "${DIPEXAR}/ad_mo_sync.log"
    )
    echo running imports_ad_sync
    ${VENV}/bin/python3 integrations/ad_integration/ad_sync.py
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

imports_ad_group_into_mo(){
    set -e
    BACK_UP_AND_TRUNCATE+=(
        "${DIPEXAR}/external_ad_users.log"
    )
    echo running imports_ad_group_into_mo
    ${VENV}/bin/python3 integrations/ad_integration/import_ad_group_into_mo.py --full-sync
}

exports_mox_rollekatalog(){
    set -e
    export MOX_ROLLE_MAPPING="${DIPEXAR}/cpr_mo_ad_map.csv"
    export MOX_ROLLE_OS2MO_API_KEY=$SAML_TOKEN
    export MOX_ROLLE_LOG_FILE="${DIPEXAR}/exports_mox_rollekatalog.log"

    echo running exports_mox_rollekatalog
    BACK_UP_AND_TRUNCATE+=(
        "$MOX_ROLLE_LOG_FILE"
    )

    ${VENV}/bin/python3 exporters/os2rollekatalog/os2rollekatalog_integration.py
}

exports_mox_stsorgsync(){
    set -e
    MOX_ERR_CODE=0
    BACK_UP_AND_TRUNCATE+=($(
        SETTING_PREFIX="mox_stsorgsync" source ${DIPEXAR}/tools/prefixed_settings.sh
        echo ${LOGFILE}
    ))
    echo running exports_mox_stsorgsync
    (
        # get VENV, MOX_MO_CONFIG and LOGFILE
        SETTING_PREFIX="mox_stsorgsync" source ${DIPEXAR}/tools/prefixed_settings.sh
        ${VENV}/bin/python3 -m mox_stsorgsync >> ${LOGFILE} 2>&1 || MOX_ERR_CODE=1
        echo "Last 50 lines from mox_stsorgsyncs log :"
        tail  -n 50  ${LOGFILE}
        echo "last 10 Errors from OS2sync"
        grep ERROR /var/log/os2sync/service.log | tail -n 10
        exit ${MOX_ERR_CODE}
    )
}

exports_os2mo_phonebook(){
    set -e
    :
}

exports_cpr_uuid(){
    set -e
    echo running exports_cpr_uuid
    (
        SETTING_PREFIX="cpr.uuid" source ${DIPEXAR}/tools/prefixed_settings.sh
        ${VENV}/bin/python3 exporters/cpr_uuid.py ${CPR_UUID_FLAGS}
    )
}

exports_viborg_emus(){
    set -e
    echo running viborg_emus
    ${VENV}/bin/python3 exporters/viborg_xml_emus_sftp.py
}

exports_viborg_eksterne(){
    set -e
    echo "running viborgs eksterne"
    ${VENV}/bin/python3 exporters/viborg_eksterne/viborg_eksterne.py || exit 1
    $(
        SETTING_PREFIX="mora.folder" source ${DIPEXAR}/tools/prefixed_settings.sh
        SETTING_PREFIX="integrations.ad" source ${DIPEXAR}/tools/prefixed_settings.sh
        SETTING_PREFIX="exports_viborg_eksterne" source ${DIPEXAR}/tools/prefixed_settings.sh
        system_user="${system_user%%@*}"
        [ -z "${query_export}" ] && exit 1
        [ -z "${system_user}" ] && exit 1
        [ -z "${password}" ] && exit 1
        [ -z "${destination_smb_share}" ] && exit 1
        [ -z "${destination_directory}" ] && exit 1
        [ -z "${outfile_basename}" ] && exit 1
        [ -z "${workgroup}" ] && exit 1

        cd ${query_export}
        smbclient -U "${system_user}%${password}"  \
            ${destination_smb_share} -m SMB2  \
            -W ${workgroup} --directory ${destination_directory} \
            -c 'put '${outfile_basename}''

        #smbclient -U "${system_user}%${password}"  \
        #    ${destination_smb_share} -m SMB2  \
        #    -W ${workgroup} --directory ${destination_directory} \
        #    -c 'del '${outfile_basename}''
    )
}

reports_sd_db_overview(){
    set -e
    echo running reports_sd_db_overview
    outfile=$(mktemp)
    ${VENV}/bin/python3 integrations/SD_Lon/db_overview.py > ${outfile}
    head -2 ${outfile}
    echo "..."
    tail -3 ${outfile}
    rm ${outfile}
}

reports_opus_db_overview(){
    set -e
    echo running reports_opus_db_overview
    outfile=$(mktemp)
    ${VENV}/bin/python3 integrations/opus/db_overview.py > ${outfile}
    head -4 ${outfile}
    echo "..."
    tail -3 ${outfile}
    rm ${outfile}
}

 

exports_plan2learn(){
    set -e
    echo "running exports_plan2learn"
    declare -a CSV_FILES=(
	bruger
	leder
	engagement
	organisation
	stillingskode
    )
    ${VENV}/bin/python3 ${DIPEXAR}/exporters/plan2learn/plan2learn.py
    
    (
	set -x
        # get OUT_DIR and EXPORTS_DIR
        SETTING_PREFIX="mora.folder" source ${DIPEXAR}/tools/prefixed_settings.sh
	[ -z "$query_export" ] && exit 1
	for f in "${CSV_FILES[@]}"
	do
	    ${VENV}/bin/python3 ${DIPEXAR}/exporters/plan2learn/ship_files.py \
		   ${query_export}/plan2learn_${f}.csv ${f}.csv
	done
    )
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
        cp "${WORK_DIR}"/*.csv "${EXPORTS_DIR}"
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

    if [ "${RUN_SD_UPDATE_PRIMARY}" == "true" ]; then
        imports_sd_update_primary || return 2
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

    if [ "${RUN_AD_GROUP_INTO_MO}" == "true" ]; then
        imports_ad_group_into_mo || return 2
    fi
}

# exports may also be interdependent: -e
exports(){
    [ "${IMPORTS_OK}" == "false" ] \
        && echo ERROR: imports are in error - skipping exports \
        && return 1 # exports depend on imports

    if [ "${RUN_MOX_STS_ORGSYNC}" == "true" ]; then
        exports_mox_stsorgsync || return 2
    fi

    if [ "${RUN_QUERIES_BALLERUP}" == "true" ]; then
        exports_queries_ballerup || return 2
    fi

    if [ "${RUN_EXPORT_EMUS}" == "true" ]; then
        exports_viborg_emus || return 2
    fi

    if [ "${RUN_EXPORTS_VIBORG_EKSTERNE}" == "true" ]; then
        exports_viborg_eksterne || return 2
    fi

    if [ "${RUN_EXPORTS_OS2MO_PHONEBOOK}" == "true" ]; then
        exports_os2mo_phonebook || return 2
    fi

    if [ "${RUN_CPR_UUID}" == "true" ]; then
        # this particular report is not allowed to fail
        exports_cpr_uuid || return 2
    fi

    if [ "${RUN_MOX_ROLLE}" == "true" ]; then
        exports_mox_rollekatalog || return 2
    fi

    if [ "${RUN_PLAN2LEARN}" == "true" ]; then
        exports_plan2learn || return 2
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
    temp_report=$(mktemp)
    for f in ${BACK_UP_BEFORE_JOBS[@]}
    do
        FILE_FAILED=false
        # try to append to tar file and report if not found
        tar -rf $BUPFILE "${f}" > ${temp_report} 2>&1 || FILE_FAILED=true
        if [ "${FILE_FAILED}" = "true" ]; then
            BACKUP_OK=false
            echo BACKUP ERROR
            cat ${temp_report}
        fi
    done
    rm ${temp_report}
    declare -i age=$(stat -c%Y ${BUPFILE})-$(stat -c%Y ${SNAPSHOT_LORA})
    if [[ ${age} -gt ${BACKUP_MAX_SECONDS_AGE} ]]; then
        BACKUP_OK=false 
        echo "ERROR database snapshot is more than ${BACKUP_MAX_SECONDS_AGE} seconds old: $age"
    fi
}

post_backup(){
    temp_report=$(mktemp)
    for f in ${BACK_UP_AFTER_JOBS[@]} ${BACK_UP_AND_TRUNCATE[@]}
    do
        FILE_FAILED=false
        # try to append to tar file and report if not found
        tar -rf $BUPFILE "${f}" > ${temp_report} 2>&1 || FILE_FAILED=true
        if [ "${FILE_FAILED}" = "true" ]; then
            BACKUP_OK=false
            echo BACKUP ERROR
            cat ${temp_report}
        fi
    done
    rm ${temp_report}
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
    echo backup done # do not remove this line
}

show_status(){
    echo IMPORTS_OK=${IMPORTS_OK}
    echo EXPORTS_OK=${EXPORTS_OK}
    echo REPORTS_OK=${REPORTS_OK}
    if [ "${1}" = "post_backup" ]; then
        echo BACKUP_OK=${BACKUP_OK}
        echo
        return
    fi
    echo
    echo Hvilke jobs er slået til/fra/udkommenterede:
    echo
    grep 'crontab.*RUN_.*' settings/settings.json
    echo
    echo "Import/export software:"
    show_git_commit
    echo "Os2mo-software"
    echo
    grep image: /opt/docker/os2mo/docker-compose.yml
    echo
    echo "Os2Sync software"
    echo
    grep image: /opt/docker/os2sync/docker-compose.yml
    echo "    mox_stsorgsync: $(head -1 ../mox_stsorgsync/NEWS)"
    echo
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

            [ -r "${SVC_KEYTAB}" ] || echo WARNING: cannot read keytab

            kinit ${SVC_USER} -k -t ${SVC_KEYTAB} || (
                echo WARNING: not able to refresh kerberos auth - authentication failure
            )
        else
            echo WARNING: not able to refresh kerberos auth - username or keytab missing
        fi

        export BUPFILE=${CRON_BACKUP}/$(date +%Y-%m-%d-%H-%M-%S)-cron-backup.tar

        pre_backup
        imports && IMPORTS_OK=true
        exports && EXPORTS_OK=true
        reports && REPORTS_OK=true
        echo
        show_status
        post_backup
        show_status post_backup > ${CRON_LOG_FILE}_status
    ) > ${CRON_LOG_FILE} 2>&1

    # write directly on stdout for mail-log
    cat ${CRON_LOG_FILE}_status
    cat ${CRON_LOG_FILE}
     
elif [ -n "$(grep $1\(\) $0)" ]; then
    echo running single job function
    $1
fi
