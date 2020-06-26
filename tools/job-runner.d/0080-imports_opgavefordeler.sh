imports_opgavefordeler(){
    set -e
    BACK_UP_AND_TRUNCATE+=(
        "${DIPEXAR}/opgavefordeler.log"
    )
    echo running imports_opgavefordeler
    "${VENV}/bin/python3" integrations/kle/opgavefordeler.py
}

