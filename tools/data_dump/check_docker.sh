#!/bin/bash
set -eou pipefail

CONTAINER_NAME="$1"

if ! [ -x "$(command -v docker)" ]; then
    echo "Unable to locate the 'docker' executable."
    exit 1
fi
if [ ! "$(docker ps -q -f name="${CONTAINER_NAME}")" ]; then
    echo "Unable to locate a running database container: ${CONTAINER_NAME}"
    exit 1
fi
