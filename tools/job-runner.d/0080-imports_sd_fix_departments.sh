imports_sd_fix_departments(){
    set -e
    BACK_UP_AND_TRUNCATE+=(
        "${DIPEXAR}/fix_sd_departments.log"
    )
    echo running imports_sd_fix_departments
    ${VENV}/bin/python3 integrations/SD_Lon/sd_fix_departments.py
}

