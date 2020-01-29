#!/bin/bash
# this script will try to update the whole
# os2mo-data-import-and-export-directory
# installing/upgrading all dependencies

export DIPEXAR=${DIPEXAR:=$(cd $(dirname $0); pwd )/..}
export VENV=${VENV:=${DIPEXAR}/venv}
cd ${DIPEXAR}

[ -d venv ] || python3 -m venv venv
[ -d ../backup ] || mkdir ../backup
[ -d ./tmp ] || mkdir ./tmp

git pull

venv/bin/pip install pip --upgrade
find . -name 'requirements.*' | grep -v venv/ | while read REQFILE
do
    echo installing ${REQFILE}
    venv/bin/pip install -r $REQFILE --upgrade
done
venv/bin/pip install ./os2mo_data_import --upgrade
