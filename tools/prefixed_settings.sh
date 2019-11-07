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
    set +x
    jq -r 'to_entries|map("\(.key)=\(.value|tostring)")[]' ${CUSTOMER_SETTINGS} \
    | sed -e 's/^'${SETTING_PREFIX}'\.//' \
    | while IFS="=" read key val
    do  # only keys with exactly this prefix - no dots
        [ "${key}" == "${key/\./X}" ] && echo export ${key}=\"$val\"
    done
)
