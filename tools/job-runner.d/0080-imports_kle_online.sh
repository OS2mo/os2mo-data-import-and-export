imports_kle_online(){
    set -e
    BACK_UP_AND_TRUNCATE+=(
        "${DIPEXAR}/kle_online.log"
    )
    echo running imports_kle_online
    "${VENV}/bin/python3" os2mo_data_import/kle/kle_import.py
}

