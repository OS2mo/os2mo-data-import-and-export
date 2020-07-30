#!/bin/bash

CLI="python mox_util.py cli"

TOP_LEVEL_UUID=$($CLI ensure-facet-exists --bvn hoved_organisation --description Hovedorganisation)
echo "${TOP_LEVEL_UUID}"
$CLI ensure-facet-exists --bvn faglig_organisation --description "Faglig organisation"

$CLI ensure-class-exists --bvn lo --title "LO" --facet-bvn hoved_organisation
$CLI ensure-class-exists --bvn metal --title "Dansk Metal" --facet-bvn faglig_organisation --parent-bvn lo
$CLI ensure-class-exists --bvn 3f --title "3F" --facet-bvn faglig_organisation --parent-bvn lo

# Configure MO to utilize newly created facet
curl -X POST -H "Content-Type: application/json" --data "{\"org_units\": {\"association_extra_uuid\": \"${TOP_LEVEL_UUID}\"}}" http://localhost:5000/service/configuration
