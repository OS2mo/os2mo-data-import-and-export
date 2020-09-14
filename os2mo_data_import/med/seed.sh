#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "${DIR}"

# CLI="echo python mox_util.py cli"
CLI="python mox_util.py cli"
# Format of the source file is a recursive tree, with entries:
# "BVN": {
#     "title": "Class title",
#     "description": "Class description",
#     "children": {...} // dict of entries alike this one
# }
# Note: BVNs must be globally unique, and valid variable names
SOURCE=$(cat "seed.json")

echo "Creating layer facets"
$CLI ensure-facet-exists --bvn hovedorg --description Hovedorganisation
$CLI ensure-facet-exists --bvn fagorg --description Fagorganisation

declare -A LAYERS
LAYERS[1]="hovedorg"
LAYERS[2]="fagorg"

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
create_tree "." "" 1

# Configure MO to utilize newly created facet
TOP_LEVEL_UUID=$($CLI ensure-facet-exists --bvn hovedorg --description Hovedorganisation | cut -f1 -d' ')
curl -X POST -H "Content-Type: application/json" --data "{\"org_units\": {\"association_dynamic_facets\": \"${TOP_LEVEL_UUID}\"}}" http://localhost:5000/service/configuration
curl -X POST -H "Session: ${SAML_TOKEN}" -H "Content-Type: application/json" --data "{\"org_units\": {\"association_dynamic_facets\": \"${TOP_LEVEL_UUID}\"}}" http://localhost:5000/service/configuration
