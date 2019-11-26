#!/bin/bash
# renew-utility for keytab - this program must be run interactively
# set principal and password in environment before running this script
# it will subsequently replace the 'user.key' keytab in the ~/CRON directory
if [ -z "${principal}" -o -z "${passwd}" ]; then
    printf "\nYou must export principal and passwd in environment like\n"
    echo export principal=\"asdf@ASDF.COM\"
    echo export passwd=\"secret\"
    echo program terminated
    exit 1
fi
echo ${principal}
echo ${passwd}

# test normal login with password
rm trace-no-key 2>/dev/null
export KRB5_TRACE=trace-no-key
kinit -VVV ${principal}
klist -e

# specify enctyption keys and find password version
declare -a enctypes=(
	aes256-cts-hmac-sha1-96
	arcfour-hmac
)
kvno=$(kvno ${principal} | awk '{print $NF}')
# remove cached ticket
kdestroy

# test with all encryption types
for enctype in "${enctypes[@]}" ; do
    printf "**** enter the following 4 lines into ktutil: coming up ****\n\n"
    echo "addent -password -p ${principal} -k ${kvno} -e ${enctype}"
    echo $passwd
    rm user.key-${kvno}-enc-${enctype} trace-kvno-${kvno}-enc-${enctype} 2>/dev/null
    echo wkt user.key-${kvno}-enc-${enctype}
    echo quit
    printf "\n********\n"
    export KRB5_TRACE=trace-kvno-${kvno}-enc-${enctype}
    ktutil
    kinit -VVV -k -t user.key-${kvno}-enc-${enctype} ${principal}
    # diff <(cut -d' ' -f3-100 trace-no-key) <(cut -d' ' -f3-100 trace-kvno-${kvno})
    klist -e \
        && printf "\n\n*** success ^^^ ***\n\n" \
        && mv user.key-${kvno}-enc-${enctype} ~/CRON/user.key \
        && exit 0 \
        || printf "\n\n*** failed  ^^^ ***\n\n"
    kdestroy
done

