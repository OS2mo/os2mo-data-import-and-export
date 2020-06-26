imports_test_ad_connectivity(){
    set -e
    BACK_UP_AND_TRUNCATE+=(
        "${DIPEXAR}/test_connectivity.log"
    )
    echo running imports_test_ad_connectivity
    ${VENV}/bin/python3 integrations/ad_integration/test_connectivity.py  --test-read-settings
}

