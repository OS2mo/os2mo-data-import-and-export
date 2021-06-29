#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "${DIR}"

MOX_URL="${MOX_URL:-http://localhost:8080}"
MORA_URL="${MORA_URL:-http://localhost:5000}"
# CLI="echo python mox_util.py cli"
CLI="venv/bin/python os2mo_data_import/mox_helpers/mox_util.py cli --mox-base ${MOX_URL}"

${CLI} bulk-ensure frederikshavn.json

# Configure MO to utilize newly created facet
TOP_LEVEL_UUID=$($CLI ensure-facet-exists --bvn "medarbejder_organisation3" | cut -f1 -d' ')
echo "${TOP_LEVEL_UUID}"
curl -X POST -H "Content-Type: application/json" --data "{\"org_units\": {\"association_dynamic_facets\": \"${TOP_LEVEL_UUID}\"}}" ${MORA_URL}/service/configuration
curl -X POST -H "Session: ${SAML_TOKEN}" -H "Content-Type: application/json" --data "{\"org_units\": {\"association_dynamic_facets\": \"${TOP_LEVEL_UUID}\"}}" ${MORA_URL}/service/configuration
