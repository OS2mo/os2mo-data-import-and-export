imports_ad_sync(){
    set -e
    BACK_UP_AND_TRUNCATE+=(
        "${DIPEXAR}/ad_mo_sync.log"
    )
    echo running imports_ad_sync
    ${VENV}/bin/python3 integrations/ad_integration/ad_sync.py
}

