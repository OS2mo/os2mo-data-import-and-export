#!/bin/bash
set +x
export DIPEXAR=${DIPEXAR:=$(cd $(dirname $0); pwd )/..}
export CUSTOMER_SETTINGS=${CUSTOMER_SETTINGS:=${DIPEXAR}/settings/settings.json}
export SETTINGS_FILE=$(basename ${CUSTOMER_SETTINGS})
export VENV=${VENV:=${DIPEXAR}/venv}
source ${DIPEXAR}/tools/prefixed_settings.sh

bupfile="$1"
if [ -z "${bupfile}" ]; then
    echo no backup tar.gz file specified
    exit 2
fi

# resttore database
restore_lora_db(){
    (
    SETTING_PREFIX= \
    CUSTOMER_SETTINGS="${LORA_CONFIG}" \
    source ${DIPEXAR}/tools/prefixed_settings.sh
    tar -xOf ${bupfile} opt/magenta/snapshots/os2mo_database.sql \
    | PGPASSWORD=${DB_PASSWORD} psql -d mox -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER}
    )
}

restore_sd_run_db(){
    set -x
    SD_IMPORT_RUN_DB=$(
        SETTING_PREFIX="integrations.SD_Lon.import" \
        source ${DIPEXAR}/tools/prefixed_settings.sh
        echo ${run_db}
    )
    tar -xOf ${bupfile} ${SD_IMPORT_RUN_DB#/} > $SD_IMPORT_RUN_DB
}

restore_lora_db
restore_sd_run_db
