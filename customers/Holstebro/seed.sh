#!/bin/bash
MOX_URL="${MOX_URL:-http://localhost:5000/lora}"
MORA_URL="${MORA_URL:-http://localhost:5000}"
CLI="python os2mo_data_import/mox_helpers/mox_util.py cli --mox-base ${MOX_URL}"

${CLI} bulk-ensure customers/Holstebro/holstebro.json

# Configure MO to utilize newly created facet
TOP_LEVEL_UUID=$($CLI ensure-facet-exists --bvn "hovedorganisation" | cut -f1 -d' ')
echo "${TOP_LEVEL_UUID} - Put this as association_dynamic_facets in OS2MOs configuration"
