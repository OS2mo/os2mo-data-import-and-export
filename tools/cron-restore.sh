#!/bin/bash
. tools/job-runner.sh
if [ "$EUID" -ne 0 -o "${JOB_RUNNER_MODE}" != "sourced" ]; then
    echo this script must be run as user root from the root of the os2mo-data-import-and-export folder
    exit 1
fi

bupfile="$1"

if [ -z "${OS2MO_COMPOSE_YML}" ]; then
    echo no docker compose file for os2mo specified
    exit 2
fi

if [ -z "${SNAPSHOT_LORA}" ]; then
    echo no lora snapshot specd
    exit 2
fi

if [ -z "${bupfile}" ]; then
    echo no backup tar.gz file specified
    exit 2
fi

if [ ! -f "${bupfile}" ]; then
    echo backup file not found
    exit 2
fi

# check that backup is valid: imports have to have passesd
check_restore_validity(){
    echo "FIXME: currently backup validity is not probed for"
    echo "       look for IMPORTS_OK file in backup.tar.gz"
    echo "       sql file of reasonable size, and other needed files"
    echo "       also check that the mox database is empty"
}

# restore lora database
restore_lora_db(){
    source ${DIPEXAR}/tools/prefixed_settings.sh
    tar -xOf ${bupfile} "${SNAPSHOT_LORA#/}" > "${SNAPSHOT_LORA}" || exit 1
    docker-compose -f "${OS2MO_COMPOSE_YML}" exec mox python3 -m oio_rest truncatedb
    docker-compose -f "${OS2MO_COMPOSE_YML}" exec -u postgres mox-db bash -c 'psql mox < /database_snapshot/'${SNAPSHOT_LORA##*/}
}

# restore run-db so import knows where it is at
restore_sd_run_db(){
    RUN_DB=$(SETTING_PREFIX="integrations.SD_Lon.import" source ${DIPEXAR}/tools/prefixed_settings.sh; echo ${run_db})
    if [ -z "$RUN_DB" ]; then
        RUN_DB=$(SETTING_PREFIX="integrations.opus.import" source ${DIPEXAR}/tools/prefixed_settings.sh; echo ${run_db})
    fi
    if [ -z "$RUN_DB" ]; then
        exit 2
    fi
    tar -xOf ${bupfile} ${RUN_DB#/} > $RUN_DB
}

# restore the map between cpr and uuid
restore_cpr_mo_ad_map_to_cpr_uuid_map(){
    echo cpr_mo_ad_map.csv is not restored. You are supposed to always use the latest
    echo Should the file be missing and You still have a valid database You should create it 
    echo by running "job-runner.sh reports_cpr_uuid"
    echo The resulting file should be compared to to settings/cpr_uuid_map.csv
    echo All lines in cpr_mo_ad_map.csv must be in settings/cpr_uuid_map.csv
    # settings/cpr_uuid_map.csv is the one that is read from during import
    # cpr_mo_ad_map.csv is the one that is written to during export
    # tar -xOf ${bupfile} cpr_mo_ad_map.csv > ${DIPEXAR}/cpr_mo_ad_map-restored.csv
    :
}

check_restore_validity || exit 2
restore_lora_db
restore_sd_run_db
restore_cpr_mo_ad_map_to_cpr_uuid_map
