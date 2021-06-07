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



# NOTE: If you get an error, check if the following packages are installed:
# * sudo apt-get install unixodbc-dev freetds-dev unixodbc tdsodbc libkrb5-dev libmysqlclient-dev cifs-utils
# Required for development
#
# You might also need:
# $ pip install --upgrade cython

/code/venv/bin/pip install pip --upgrade
find . -name 'requirements.txt' | grep -v venv/ | while read REQFILE
do
    echo installing ${REQFILE}
    venv/bin/pip install -r $REQFILE --upgrade
done
venv/bin/pip install -r ./integrations/requirements/common.txt
venv/bin/pip install -r ./integrations/requirements/test.txt
venv/bin/pip install ./os2mo_data_import --upgrade

# Install 'metacli' into venv
source venv/bin/activate
venv/bin/pip install --editable .