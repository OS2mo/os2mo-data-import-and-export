#!/bin/bash
# renew-utility for keytab - this program must be run interactively
# set system_user and password in environment before running this script
# it will subsequently replace the 'user.key' keytab in the ~/CRON directory
export DIPEXAR=${DIPEXAR:=$(cd $(dirname $0); pwd )/..}
export CUSTOMER_SETTINGS=${CUSTOMER_SETTINGS:=${DIPEXAR}/settings/settings.json}
export SETTINGS_FILE=$(basename ${CUSTOMER_SETTINGS})
SETTING_PREFIX="integrations.ad" source ${DIPEXAR}/tools/prefixed_settings.sh


# make tmp and use it
[ -d "${DIPEXAR}/tmp" ] || mkdir "${DIPEXAR}/tmp"
cd "${DIPEXAR}/tmp"

# test normal login with password
echo ${password}
rm trace-no-key 2>/dev/null
export KRB5_TRACE=trace-no-key
kinit -VVV ${system_user}
klist -e

# specify enctyption keys and find password version
declare -a enctypes=(
	aes256-cts-hmac-sha1-96
	arcfour-hmac
)
declare -a kvnos=($(kvno ${system_user} | awk '{print $NF}'))
[ -n "${kvnos[@]}" ] || kvnos=(1 2 3 4 5 6 7)
# remove cached ticket
kdestroy

# test for some/one specific kvno
for kvno in "${kvnos[@]}" ; do
    # test with all encryption types
    for enctype in "${enctypes[@]}" ; do
        printf "**** enter the following 4 lines into ktutil: coming up ****\n\n"
        echo "addent -password -p ${system_user} -k ${kvno} -e ${enctype}"
        echo $password
        rm user.key-${kvno}-enc-${enctype} trace-kvno-${kvno}-enc-${enctype} 2>/dev/null
        echo wkt user.key-${kvno}-enc-${enctype}
        echo quit
        printf "\n********\n"
        export KRB5_TRACE=trace-kvno-${kvno}-enc-${enctype}
        ktutil
        kinit -VVV -k -t user.key-${kvno}-enc-${enctype} ${system_user}
        # diff <(cut -d' ' -f3-100 trace-no-key) <(cut -d' ' -f3-100 trace-kvno-${kvno})
        klist -e \
            && printf "\n\n*** success ^^^ ***\n\n" \
            && mv user.key-${kvno}-enc-${enctype} ~/CRON/user.key \
            && exit 0 \
            || printf "\n\n*** failed  ^^^ ***\n\n"
        kdestroy
    done
done
