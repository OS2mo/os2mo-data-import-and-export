exports_os2sync(){
    set -e
    BACK_UP_AND_TRUNCATE+=($(
        SETTING_PREFIX="os2sync" source ${DIPEXAR}/tools/prefixed_settings.sh
        echo ${log_file}
    ))
    echo running exports_os2sync
    ${VENV}/bin/python3 -m integrations.os2sync
}

