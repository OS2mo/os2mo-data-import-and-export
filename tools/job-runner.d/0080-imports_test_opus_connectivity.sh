imports_test_opus_connectivity(){
    set -e
    echo running imports_test_ops_connectivity
    ${VENV}/bin/python3 integrations/opus/test_opus_connectivity.py --test-diff-import
}

