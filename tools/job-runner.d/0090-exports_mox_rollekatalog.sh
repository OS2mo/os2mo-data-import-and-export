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

