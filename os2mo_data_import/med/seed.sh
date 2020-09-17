#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "${DIR}"

MOX_URL="${MOX_URL:-'http://localhost:8080'}"
# CLI="echo python mox_util.py cli"
CLI="python mox_util.py cli --mox-base ${MOX_URL}"
# Format of the source file is a recursive tree, with entries:
# "BVN": {
#     "title": "Class title",
#     "description": "Class description",
#     "children": {...} // dict of entries alike this one
# }
# Note: BVNs must be globally unique, and valid variable names
SOURCE=$(cat "seed.json")
SOURCE_LAYERS=$(cat "layers.json")

echo "Creating layer facets"
declare -A LAYERS
NUM_LAYERS=$(echo ${SOURCE_LAYERS} | jq ".|length")
for i in $(seq 0 $((NUM_LAYERS - 1))); do
   BVN=$(echo ${SOURCE_LAYERS} | jq -r ".[${i}].bvn")
   DESCRIPTION=$(echo ${SOURCE_LAYERS} | jq -r ".[${i}].description")
   $CLI ensure-facet-exists --bvn "${BVN}" --description "${DESCRIPTION}"
   LAYERS[${i}]="${BVN}"
done

printarr() { declare -n __p="$1"; for k in "${!__p[@]}"; do printf "%s=%s\n" "$k" "${__p[$k]}" ; done ;  } 
printarr LAYERS
echo ""

create_tree()
{
    local FILTER=$1
    local PARENT_BVN=$2
    local LAYER=$3
    local PARENT_FACET="${LAYERS[$LAYER]}"
    local FILTERED=$(echo "${SOURCE}" | jq "${FILTER}")
    local NUM_KEYS=$(echo "${FILTERED}" | jq "keys|length")
    if [ "${NUM_KEYS}" -eq 0 ]; then
       return
    fi
    local KEYS=$(echo "${FILTERED}" | jq -r "keys | .[]")
    #echo "${PARENT} - ${NUM_KEYS}"
    while IFS= read -r KEY; do
        local BVN=$KEY
        local TITLE=$(echo "${FILTERED}" | jq -r ".${BVN}.title")
        echo "Creating '${BVN}' - '${TITLE}' on layer ${LAYER}"
        local DESCRIPTION=$(echo "${FILTERED}" | jq -r ".${BVN}.description")
        if [ -z ${PARENT_BVN} ]; then
            ${CLI} ensure-class-exists --bvn "${BVN}" --title "${TITLE}" --description "${DESCRIPTION}" --facet-bvn "${PARENT_FACET}"
        else
            ${CLI} ensure-class-exists --bvn "${BVN}" --title "${TITLE}" --description "${DESCRIPTION}" --facet-bvn "${PARENT_FACET}" --parent-bvn "${PARENT_BVN}"
        fi
        local NEW_FILTER=$(echo "${FILTER}.${BVN}.children" | sed "s/\.\./\./g")
        # echo "${NEW_FILTER}"
        local NEW_LAYER=$((LAYER + 1))
        create_tree "${NEW_FILTER}" "${BVN}" "${NEW_LAYER}"
    done <<< "$KEYS"
    echo ""
}

echo "Creating class tree"
create_tree "." "" 0

# Configure MO to utilize newly created facet
TOP_LEVEL_UUID=$($CLI ensure-facet-exists --bvn "${LAYERS[0]}" | cut -f1 -d' ')
MORA_URL="${MORA_URL:-'http://localhost:5000'}"
curl -X POST -H "Content-Type: application/json" --data "{\"org_units\": {\"association_dynamic_facets\": \"${TOP_LEVEL_UUID}\"}}" ${MORA_URL}/service/configuration
curl -X POST -H "Session: ${SAML_TOKEN}" -H "Content-Type: application/json" --data "{\"org_units\": {\"association_dynamic_facets\": \"${TOP_LEVEL_UUID}\"}}" ${MORA_URL}/service/configuration
