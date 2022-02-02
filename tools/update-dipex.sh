#!/bin/bash
#Install dependencies for dipex

export DIPEXAR=${DIPEXAR:=$(cd $(dirname $0); pwd )/..}
export VENV=${VENV:=${DIPEXAR}/.venv}
cd ${DIPEXAR}

[ -d ../backup ] || mkdir ../backup
[ -d ./tmp ] || mkdir ./tmp
#Send git_info to prometheus
source tools/job-runner.sh
prometrics-git

#Add githooks
find .git/hooks -type l -exec rm {} \; && find .githooks -type f -exec ln -sf ../../{} .git/hooks/ \;

export POETRY_VIRTUALENVS_CREATE=true
export POETRY_VIRTUALENVS_IN_PROJECT=true
poetry install --no-interaction
source .venv/bin/activate

pip install ./os2mo_data_import --upgrade

pip install --editable .
