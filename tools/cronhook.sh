#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
SECTION="${1}"
DIPEX="$(dirname ${DIR})"
VENV="$DIPEX/venv"


# read cronhook settings
export SETTING_PREFIX="cronhook"
export CUSTOMER_SETTINGS="${DIPEX}/settings/settings.json"
. <(${VENV}/bin/python ${DIR}/prefixed_settings.py)


# run script in section if it is enabled
for script in ${DIR}/cronhook.${SECTION}.d/*.sh
do
    # get name of script-switch
    script_on=$(/usr/bin/basename -s ".sh" ${script})_on
    [ "${!script_on}" = "true" ] && (
    echo running $script
    bash -x ${script}
    )
done
echo
