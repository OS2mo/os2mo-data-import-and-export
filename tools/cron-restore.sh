#!/bin/bash
set +x
export DIPEXAR=${DIPEXAR:=$(cd $(dirname $0); pwd )/..}
export CUSTOMER_SETTINGS=${CUSTOMER_SETTINGS:=${DIPEXAR}/settings/settings.json}
export SETTINGS_FILE=$(basename ${CUSTOMER_SETTINGS})
export VENV=${VENV:=${DIPEXAR}/venv}
source ${DIPEXAR}/tools/prefixed_settings.sh

cd "${DIPEXAR}"

bupfile="$1"

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
    echo "You have to do this first:"
    echo "sudo systemctl restart postgresql; sudo salt-call state.apply magenta.mox_releases.recreatedb; sudo systemctl restart oio_rest; sudo systemctl restart mora"
}

# restore lora database
restore_lora_db(){
    (
    SETTING_PREFIX= \
    CUSTOMER_SETTINGS="${LORA_CONFIG}" \
    source ${DIPEXAR}/tools/prefixed_settings.sh
    tar -xOf ${bupfile} opt/magenta/snapshots/os2mo_database.sql \
    | PGPASSWORD=${DB_PASSWORD} psql -d mox -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER}
    )
}

# restore run-db so import knows where it is at
restore_sd_run_db(){
    set -x
    SD_IMPORT_RUN_DB=$(
        SETTING_PREFIX="integrations.SD_Lon.import" \
        source ${DIPEXAR}/tools/prefixed_settings.sh
        echo ${run_db}
    )
    tar -xOf ${bupfile} ${SD_IMPORT_RUN_DB#/} > $SD_IMPORT_RUN_DB
}

# restore the map between cpr and uuid
restore_cpr_mo_ad_map_to_cpr_uuid_map(){
    # settings/cpr_uuid_map.csv is the one that is read from during import
    # cpr_mo_ad_map.csv is the one that is written to during export
    tar -xOf ${bupfile} cpr_mo_ad_map.csv > ${DIPEXAR}/settings/cpr_uuid_map.csv
    tar -xOf ${bupfile} cpr_mo_ad_map.csv > ${DIPEXAR}/cpr_mo_ad_map.csv
}

check_restore_validity || exit 2
restore_lora_db
restore_sd_run_db
restore_cpr_mo_ad_map_to_cpr_uuid_map
