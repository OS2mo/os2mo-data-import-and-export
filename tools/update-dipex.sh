#!/bin/bash
#Install dependencies for dipex

export DIPEXAR=${DIPEXAR:=$(cd $(dirname $0); pwd )/..}
export VENV=${VENV:=${DIPEXAR}/.venv}
export POETRYPATH=${POETRYPATH:=/home/$(whoami)/.local/bin/poetry}
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
$POETRYPATH install --no-interaction

.venv/bin/pip install --editable .

cd integrations/SD_Lon/
$POETRYPATH install
