#!/bin/bash
## Script til at sætte ejer på klasser for frederikshavn

MOX_URL="${MOX_URL:-http://localhost:8080}"

CLI="venv/bin/python os2mo_data_import/mox_helpers/mox_util.py cli --mox-base ${MOX_URL}"

MORA_URL="${MORA_URL:-http://localhost:5000}"

UUID=$(curl --silent --header "SESSION: ${SAML_TOKEN}" ${MORA_URL}/service/o/ | jq -r .[0].uuid)

# Find UUID'er med: curl localhost:5001/service/o/${UUID}/children | jq .
# og indsæt dem her:
MED="96d2125e-7f5d-454a-a564-ce8ccb0b2d95"
MAIN="7ddf4346-ce24-6ba5-7620-a1e7162fda68"

# Find alle klasser med facetten org_unit_type
CLASSES=$(curl --silent --header "SESSION: ${SAML_TOKEN}" ${MORA_URL}/service/o/${UUID}/f/org_unit_type/)

TOTAL_CLASSES=$(echo $CLASSES | jq -r  .data.total)
echo "Found ${TOTAL_CLASSES} org_unit_types"

# For disse klasser skal ejerskabet være MED organisationen:
LIST='"DirektørMED" "Pers.møde m/ MED" "LokalMED" "CenterMED" "HovedMED"'


# Loop over klasserne
echo $CLASSES | jq -c .data.items[] | while read line; do
    NAME=$(echo "$line" | jq .name)
    BVN=$(echo "$line" | jq .user_key)
    # Fjern "
    NAME=${NAME//\"/}
    BVN=${BVN//\"/}

    # Tjek om navnet er i listen og sæt i så fald ejeren til MED-organisationen
    if [[ -n $(echo $LIST | grep -w "$NAME") ]]; then
        OWNER=$MED
    else
        OWNER=$MAIN
    fi
    echo "$NAME : $OWNER"
        
    ${CLI} ensure-class-value --bvn "${BVN}" --variable ejer --new_value "$OWNER" 
done
