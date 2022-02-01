#!/bin/bash
# this script will try to update the whole
# os2mo-data-import-and-export-directory
# installing/upgrading all dependencies

export DIPEXAR=${DIPEXAR:=$(cd $(dirname $0); pwd )/..}
export VENV=${VENV:=${DIPEXAR}/.venv}
cd ${DIPEXAR}

[ -d ../backup ] || mkdir ../backup
[ -d ./tmp ] || mkdir ./tmp


# show a changelog after pull overview
old_git=$(git show -s --format=%H)
git pull
new_git=$(git show -s --format=%H)
git log --pretty=oneline ${old_git}..${new_git}

#Send git_info to prometheus
source tools/job-runner.sh
prometrics-git

#Add githooks
find .git/hooks -type l -exec rm {} \; && find .githooks -type f -exec ln -sf ../../{} .git/hooks/ \;

# NOTE: If you get an error, check if the following packages are installed:
# * sudo apt-get install unixodbc-dev freetds-dev unixodbc tdsodbc libkrb5-dev libmysqlclient-dev cifs-utils
# Required for development
#
# You might also need:
# $ pip install --upgrade cython
export POETRY_VIRTUALENVS_CREATE=true
export POETRY_VIRTUALENVS_IN_PROJECT=true
poetry install --no-interaction
source .venv/bin/activate

pip install ./os2mo_data_import --upgrade

pip install --editable .