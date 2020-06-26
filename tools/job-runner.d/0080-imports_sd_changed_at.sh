imports_sd_changed_at(){
    set -e
    echo running imports_sd_changed_at
    BACK_UP_AFTER_JOBS+=(
        ${DIPEXAR}/cpr_mo_ad_map.csv
        ${DIPEXAR}/settings/cpr_uuid_map.csv
    )
    ${VENV}/bin/python3 integrations/SD_Lon/sd_changed_at.py
}

