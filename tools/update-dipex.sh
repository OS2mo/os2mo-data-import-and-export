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


# show a changelog after pull overview
old_git=$(git show -s --format=%H)
git pull
new_git=$(git show -s --format=%H)
git log --pretty=oneline ${old_git}..${new_git}

# TODO: Check if the following packages are installed:
# * unixodbc-dev freetds-dev unixodbc tdsodbc libkrb5-dev libmysqlclient-dev
# Required for development
# You might also need:
# $ pip install --upgrade cython

venv/bin/pip install pip --upgrade
find . -name 'requirements.*' | grep -v venv/ | while read REQFILE
do
    echo installing ${REQFILE}
    venv/bin/pip install -r $REQFILE --upgrade
done
venv/bin/pip install ./os2mo_data_import --upgrade
