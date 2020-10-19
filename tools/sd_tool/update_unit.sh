#!/bin/bash

export DIPEXAR=${DIPEXAR:=$(cd $(dirname $0); pwd )/../..}
export VENV=${VENV:=${DIPEXAR}/venv}
cd ${DIPEXAR}

export CUSTOMER_SETTINGS=${CUSTOMER_SETTINGS:=${DIPEXAR}/settings/settings.json}
source tools/prefixed_settings.sh
export PYTHONPATH=$PWD:$PYTHONPATH

venv/bin/python integrations/SD_Lon/fix_departments.py --department-uuid="$1"
