#!/bin/bash
[ "${BASH_SOURCE[0]}" == "${0}" ] && JOB_RUNNER_MODE=running || JOB_RUNNER_MODE=sourced
[ "${JOB_RUNNER_MODE}" == "running" ] && set +x
export JOB_RUNNER_MODE
export DIPEXAR=${DIPEXAR:=$(realpath -L $(dirname $(realpath -L "${BASH_SOURCE}"))/..)}
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
    # if the json-log-tee file is present - take that too
    ${CRON_LOG_JSON}
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

imports_test_sd_connectivity(){
    set -e
    echo running imports_test_sd_connectivity
    ${VENV}/bin/python3 integrations/SD_Lon/test_sd_connectivity.py
}

imports_test_opus_connectivity(){
    set -e
    echo running imports_test_ops_connectivity
    ${VENV}/bin/python3 integrations/opus/test_opus_connectivity.py --test-diff-import
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

imports_kle_online(){
    set -e
    BACK_UP_AND_TRUNCATE+=(
        "${DIPEXAR}/kle_online.log"
    )
    echo running imports_kle_online
    "${VENV}/bin/python3" os2mo_data_import/kle/kle_import.py
}

imports_opgavefordeler(){
    set -e
    BACK_UP_AND_TRUNCATE+=(
        "${DIPEXAR}/opgavefordeler.log"
    )
    echo running imports_opgavefordeler
    "${VENV}/bin/python3" integrations/kle/opgavefordeler.py
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

exports_os2sync(){
    set -e
    BACK_UP_AND_TRUNCATE+=($(
        SETTING_PREFIX="os2sync" source ${DIPEXAR}/tools/prefixed_settings.sh
        echo ${log_file}
    ))
    echo running exports_os2sync
    ${VENV}/bin/python3 -m integrations.os2sync
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
    ${VENV}/bin/python3 exporters/emus/viborg_xml_emus_sftp.py
}

exports_viborg_eksterne(){
    set -e
    echo "running viborgs eksterne"
    ${VENV}/bin/python3 exporters/viborg_eksterne/viborg_eksterne.py --lora|| exit 1
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
    ${VENV}/bin/python3 ${DIPEXAR}/exporters/plan2learn/plan2learn.py --lora
    
    (
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

exports_actual_state_export(){
    # kører en test-kørsel
    BACK_UP_AND_TRUNCATE+=(sql_export.log)
    ${VENV}/bin/python3 ${DIPEXAR}/exporters/sql_export/sql_export.py
}

exports_historic_sql_export(){
    BACK_UP_AND_TRUNCATE+=(sql_export_historic.log)
    ${VENV}/bin/python3 ${DIPEXAR}/exporters/sql_export/sql_export.py --historic
}

exports_os2phonebook_export(){
    # kører en test-kørsel
    BACK_UP_AND_TRUNCATE+=(os2phonebook_export.log)
    ${VENV}/bin/python3 ${DIPEXAR}/exporters/os2phonebook/os2phonebook_export.py sql-export
    ${VENV}/bin/python3 ${DIPEXAR}/exporters/os2phonebook/os2phonebook_export.py generate-json
    ${VENV}/bin/python3 ${DIPEXAR}/exporters/os2phonebook/os2phonebook_export.py transfer-json
}

exports_sync_mo_uuid_to_ad(){
    BACK_UP_AND_TRUNCATE+=(sync_mo_uuid_to_ad.log)
    ${VENV}/bin/python3 ${DIPEXAR}/integrations/ad_integration/sync_mo_uuid_to_ad.py --sync-all
}

reports_viborg_managers(){
    ${VENV}/bin/python3 ${DIPEXAR}/reports/viborg_managers.py
}

exports_test(){
    set -e
    :
}


# read the run-job script et al
for module in tools/job-runner.d/*.sh; do
    echo sourcing $module
    source $module 
done


# imports are typically interdependent: -e
imports(){
    [ "${BACKUP_OK}" == "false" ] \
        && echo ERROR: backup is in error - skipping imports \
        && return 1 # imports depend on backup

    if [ "${RUN_MOX_DB_CLEAR}" == "true" ]; then
        run-job imports_mox_db_clear || return 2
    fi

    if [ "${RUN_CHECK_AD_CONNECTIVITY}" == "true" ]; then
        run-job imports_test_ad_connectivity || return 2
    fi

    if [ "${RUN_CHECK_SD_CONNECTIVITY}" == "true" ]; then
        run-job imports_test_sd_connectivity || return 2
    fi

    if [ "${RUN_CHECK_OPUS_CONNECTIVITY}" == "true" ]; then
        run-job imports_test_opus_connectivity || return 2
    fi

    if [ "${RUN_SD_FIX_DEPARTMENTS}" == "true" ]; then
        run-job imports_sd_fix_departments || return 2
    fi

    if [ "${RUN_SD_CHANGED_AT}" == "true" ]; then
        run-job imports_sd_changed_at || return 2
    fi

    if [ "${RUN_SD_UPDATE_PRIMARY}" == "true" ]; then
        run-job imports_sd_update_primary || return 2
    fi

    if [ "${RUN_OPUS_DIFF_IMPORT}" == "true" ]; then
        run-job imports_opus_diff_import || return 2
    fi

    if [ "${RUN_AD_SYNC}" == "true" ]; then
        run-job imports_ad_sync || return 2
    fi

    if [ "${RUN_BALLERUP_APOS}" == "true" ]; then
        run-job imports_ballerup_apos || return 2
    fi

    if [ "${RUN_BALLERUP_UDVALG}" == "true" ]; then
        run-job imports_ballerup_udvalg || return 2
    fi

    if [ "${RUN_AD_GROUP_INTO_MO}" == "true" ]; then
        run-job imports_ad_group_into_mo || return 2
    fi

    if [ "${RUN_KLE_ONLINE}" == "true" ]; then
        run-job imports_kle_online || return 2
    fi

    if [ "${RUN_OPGAVEFORDELER}" == "true" ]; then
        run-job imports_opgavefordeler || return 2
    fi
}

# exports may also be interdependent: -e
exports(){
    [ "${IMPORTS_OK}" == "false" ] \
        && echo ERROR: imports are in error - skipping exports \
        && return 1 # exports depend on imports

    if [ "${RUN_ACTUAL_STATE_EXPORT}" == "true" ]; then
        run-job exports_actual_state_export || return 2
    fi

    if [ "${RUN_HISTORIC_SQL_EXPORT}" == "true" ]; then
        run-job exports_historic_sql_export || return 2
    fi

    if [ "${RUN_OS2SYNC}" == "true" ]; then
        run-job exports_os2sync || return 2
    fi

    if [ "${RUN_MOX_STS_ORGSYNC}" == "true" ]; then
        run-job exports_mox_stsorgsync || return 2
    fi

    if [ "${RUN_QUERIES_BALLERUP}" == "true" ]; then
        run-job exports_queries_ballerup || return 2
    fi

    if [ "${RUN_EXPORT_EMUS}" == "true" ]; then
        run-job exports_viborg_emus || return 2
    fi

    if [ "${RUN_EXPORTS_VIBORG_EKSTERNE}" == "true" ]; then
        run-job exports_viborg_eksterne || return 2
    fi

    if [ "${RUN_EXPORTS_OS2MO_PHONEBOOK}" == "true" ]; then
        run-job exports_os2phonebook_export || return 2
    fi

    if [ "${RUN_EXPORTS_MO_UUID_TO_AD}" == "true" ]; then
        run-job exports_sync_mo_uuid_to_ad || return 2
    fi

    if [ "${RUN_CPR_UUID}" == "true" ]; then
        # this particular report is not allowed to fail
        run-job exports_cpr_uuid || return 2
    fi

    if [ "${RUN_MOX_ROLLE}" == "true" ]; then
        run-job exports_mox_rollekatalog || return 2
    fi

    if [ "${RUN_PLAN2LEARN}" == "true" ]; then
        run-job exports_plan2learn || return 2
    fi

    if [ "${RUN_EXPORTS_TEST}" == "true" ]; then
        run-job exports_test || return 2
    fi


}

# reports are typically not interdependent
reports(){
    #set -x # debug log
    [ "${IMPORTS_OK}" == "false" ] \
        && echo ERROR: imports are in error - skipping reports \
        && return 1 # reports depend on imports

    if [ "${RUN_SD_DB_OVERVIEW}" == "true" ]; then
        run-job reports_sd_db_overview || echo "error in reports_sd_db_overview - continuing"
    fi
    
    if [ "${RUN_OPUS_DB_OVERVIEW}" == "true" ]; then
        run-job reports_opus_db_overview || echo "error in reports_opus_db_overview - continuing"
    fi

    if [ "${RUN_VIBORG_MANAGERS}" == "true" ]; then
        run-job reports_viborg_managers || return 2
    fi


}

pre_truncate_logfiles(){
    # logfiles are truncated before each run as 
    [ -f "udvalg.log" ] && truncate -s 0 "udvalg.log" 
}

post_backup(){
    temp_report=$(mktemp)

    # deduplicate
    BACK_UP_AFTER_JOBS=($(printf "%s\n" "${BACK_UP_AFTER_JOBS[@]}" | sort -u))
    BACK_UP_AND_TRUNCATE=($(printf "%s\n" "${BACK_UP_AND_TRUNCATE[@]}" | sort -u))

    for f in ${BACK_UP_AFTER_JOBS[@]} ${BACK_UP_AND_TRUNCATE[@]}
    do
        FILE_FAILED=false
        # try to append to tar file and report if not found
        tar -rf $BUPFILE "${f}" > ${temp_report} 2>&1 || FILE_FAILED=true
        if [ "${FILE_FAILED}" = "true" ]; then
            BACKUP_OK=false
            run-job-log job post-backup file ! job-status failed ! file $f
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
    for f in ${BACK_UP_AND_TRUNCATE[@]}
    do
        [ -f "${f}" ] && truncate -s 0 "${f}"
    done

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
        [ "${IMPORTS_OK}" = "true" -a "${EXPORTS_OK}" = "true" -a "${REPORTS_OK}" = "true" -a "${BACKUP_OK}" = "true" ] && TOTAL_STATUS=success || TOTAL_STATUS=failed
        run-job-log job job-runner total-status ! job-status $TOTAL_STATUS \
            ! imports-ok ${IMPORTS_OK} ! exports-ok ${EXPORTS_OK} \
            ! reports-ok ${REPORTS_OK} ! backup-ok ${BACKUP_OK}
        return
    fi
    echo
    echo Hvilke jobs er slået til/fra/X-ede/udkommenterede :
    echo
    enabled_jobs=$(grep 'crontab.*RUN_.*' settings/settings.json | tr "#\",:" "X ! ")
    run-job-log job job-runner enabled-jobs ! job-status info ! ${enabled_jobs} 
    echo ${enabled_jobs} | sed 's/! */\n/g'
    echo
    run-job-log job job-runner version-info ! job-status info ! git-commit $(git show -s --format=%H)
}


if [ "${JOB_RUNNER_MODE}" == "running" -a "$#" == "0" ]; then
    (
        if [ ! -n "${CRON_LOG_JSON_SINK}" ]; then
            REASON="WARNING: crontab.CRON_LOG_JSON_SINK not specified - no json logging"
            echo ${REASON}
        fi

        if [ ! -d "${VENV}" ]; then
            REASON="FATAL: python env not found"
            run-job-log job job-runner pre-check ! job-status failed ! reason $REASON
            echo ${REASON}
            exit 2 # error
        fi

        if [ ! -n "${SVC_USER}" ]; then
            REASON="WARNING: Service user not specified"
            run-job-log job job-runner pre-check ! job-status warning ! reason $REASON
            echo ${REASON}
        fi

        if [ ! -n "${CRON_LOG_FILE}" ]; then
            REASON="FATAL: Cron log file not specified"
            run-job-log job job-runner pre-check ! job-status failed ! reason $REASON
            echo ${REASON}
            exit 2
        fi

        if [ ! -n "${CRON_BACKUP}" ]; then
            REASON="FATAL: Backup directory not specified"
            run-job-log job job-runner pre-check ! job-status failed ! reason $REASON
            echo ${REASON}
            exit 2
        fi

        if [ ! -d "${CRON_BACKUP}" ]; then
            REASON="FATAL: Backup directory non existing"
            run-job-log job job-runner pre-check ! job-status failed ! reason $REASON
            echo ${REASON}
            exit 2
        fi

        export BUPFILE=${CRON_BACKUP}/$(date +%Y-%m-%d-%H-%M-%S)-cron-backup.tar

        run-job imports && IMPORTS_OK=true
        run-job exports && EXPORTS_OK=true
        run-job reports && REPORTS_OK=true
        echo
        show_status
    ) > ${CRON_LOG_FILE} 2>&1

    # write directly on stdout for mail-log
    cat ${CRON_LOG_FILE}_status
    cat ${CRON_LOG_FILE}
    exit $TOTAL_STATUS
     
elif [ "${JOB_RUNNER_MODE}" == "running" ]; then
    if [ -n "$(grep $1\(\) $0)" ]; then
        echo running single job function
        $1
    fi
elif [ "${JOB_RUNNER_MODE}" == "sourced" ]; then
    # export essential functions
    export -f post_backup reports_opus_db_overview reports_sd_db_overview
fi
