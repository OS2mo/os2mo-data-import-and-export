#!/bin/bash
[ "${mount_opus_mountpoint}" = "" ] && echo "ERROR: mount_opus_mountpoint not in settings" && exit 1

/bin/umount "${mount_opus_mountpoint}"
