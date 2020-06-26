imports_ad_group_into_mo(){
    set -e
    BACK_UP_AND_TRUNCATE+=(
        "${DIPEXAR}/external_ad_users.log"
    )
    echo running imports_ad_group_into_mo
    ${VENV}/bin/python3 integrations/ad_integration/import_ad_group_into_mo.py --full-sync
}

