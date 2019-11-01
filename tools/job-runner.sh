#!/bin/bash
# copy and adapt to cron.sh
# specify global environment here eg. set-succes-indicators
set -x
export DIPEXAR=${DIPEXAR:=/home/andeby/os2mo-data-import-and-export}
export VENV=${VENV:=${DIPEXAR}/venv}
export IMPORTS_OK=false
export EXPORTS_OK=false
export REPORTS_OK=false

source ${DIPEXAR}/tools/prefixed_settings.sh

cd ${DIPEXAR}
if [ ! -d "${VENV}" ]; then 
    echo "python env not found"
    exit 1
fi


# imports are typically interdependent: -e
imports(){
    set -x # debug log
    set -e # interdependent - bail out on first error
    echo imports are good
}

# exports may also be interdependent: -e
exports(){
    set -x # debug log
    [ $IMPORTS_OK = false ] && return 1 # exports depend on imports
    set -e # interdependent - bail out on first error
}

# reports are typically not interdependent
reports(){
    set -x # debug log
    [ $IMPORTS_OK = false ] && return 1 # reports depend on imports
    set +e # not interdependent - continue through errors
}

if [ $# == 0 ]; then 
    imports && IMPORTS_OK=true
    exports && EXPORTS_OK=true
    reports
fi
