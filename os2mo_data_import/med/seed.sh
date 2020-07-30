#!/bin/bash

CLI=python mox_util.py cli

$CLI ensure-facet-exists --bvn hoved_organisation --description Hovedorganisation
$CLI ensure-facet-exists --bvn faglig_organisation --description "Faglig organisation"

$CLI ensure-class-exists --bvn lo --title "LO" --facet-bvn hoved_organisation
$CLI ensure-class-exists --bvn metal --title "Dansk Metal" --facet-bvn faglig_organisation --parent-bvn lo
$CLI ensure-class-exists --bvn 3f --title "3F" --facet-bvn faglig_organisation --parent-bvn lo
