imports_ballerup_udvalg(){
    set -e
    BACK_UP_AND_TRUNCATE+=(
        "${DIPEXAR}/udvalg.log"
    )
    echo running imports_ballerup_udvalg
    ${VENV}/bin/python3 integrations/ballerup/udvalg_import.py
}

