#!/bin/bash
# convert {
#    "crontab.RUN_THIS_JOB":true,
#    "crontab.RUN_THAT_JOB":false,
# }
# into 
#    export RUN_THIS_JOB=true
#    export RUN_THAT_JOB=false
#
# use like:
#    source tools/prefixed_settings.sh
# defaults work li calling like:
#     SETTING_PREFIX=crontab \
#     CUSTOMER_SETTINGS=/opt/settings/customer-settings.json \
#     source tools/prefixed_settings.sh
#
export SETTING_PREFIX=${SETTING_PREFIX:=crontab}
export CUSTOMER_SETTINGS=${CUSTOMER_SETTINGS:=/opt/settings/customer-settings.json}
. <(
    jq -r 'to_entries|map("\(.key)=\(.value|tostring)")[]'\
     ${CUSTOMER_SETTINGS} | \
     grep -e '^'${SETTING_PREFIX}'\.' | sed -e 's/.*\./export /' -e 's/=/="/' -e 's/$/"/'
)
