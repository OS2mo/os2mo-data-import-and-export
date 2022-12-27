#!/bin/bash
#Install dependencies for dipex

set -e

# Never try to fetch keys from keyring, it can cause horrible DBUS issues
export PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring

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

cd $DIPEXAR/integrations/SD_Lon/
$POETRYPATH install

cd $DIPEXAR/exporters/os2sync_export
$POETRYPATH install
