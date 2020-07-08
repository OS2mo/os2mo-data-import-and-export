#!/bin/bash

export PYTHONPATH=$PWD:$PYTHONPATH

# shellcheck disable=SC1091
source venv/bin/activate
rm tmp/*.p

export MOX_BASE=http://localhost:8080
export MORA_BASE=http://localhost:5000

export MUNICIPALITY_NAME="Ballerup Kommune"
export MUNICIPALITY_CODE=151
export ANSAT_UUID=56e1214a-330f-4592-89f3-ae3ee8d5b2e6
export BASE_APOS_URL=http://apos.balk.dk:8080/apos2-
export CREATE_UDVALGS_CLASSES=yes

export PHONE_NAMES='"41504f53-0203-001f-4158-41504f494e54":"7e118f76-2150-4fec-b09f-6428cd05802b"'

export EMAIL_NAME="41504f53-0203-0020-4158-41504f494e54"
export MAIN_PHONE_NAME="41504f53-0203-001f-4158-41504f494e54"
export ALT_PHONE_NAME="7e118f76-2150-4fec-b09f-6428cd05802b"

python3 integrations/ballerup/ballerup.py
python3 integrations/ballerup/udvalg_import.py
