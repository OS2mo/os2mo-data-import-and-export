#!/bin/bash
[ "${mount_opus_share}" = "" ] && echo "ERROR: mount_opus_share not in settings" && exit 1
[ "${mount_opus_mountpoint}" = "" ] && echo "ERROR: mount_opus_mountpoint not in settings" && exit 1
[ "${mount_opus_username}" = "" ] && echo "ERROR: mount_opus_username not in settings" && exit 1
[ "${mount_opus_password}" = "" ] && echo "ERROR: mount_opus_password not in settings" && exit 1

/bin/mount | grep "${mount_opus_share}" | grep "$mount_opus_mountpoint" > /dev/null && echo opus already mounted && exit 0

echo mounting opus

/bin/mount -vt cifs "${mount_opus_share}" "$mount_opus_mountpoint" -o "username=${mount_opus_username},password=${mount_opus_password}" > /dev/null



