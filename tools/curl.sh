#!/bin/bash
. tools/prefixed_settings.sh
curl -k -H 'SESSION: '"${SAML_TOKEN}" "$@" | jq .
echo
