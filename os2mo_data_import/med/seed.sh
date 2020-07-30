#!/bin/bash

python med_import.py cli ensure-facet-exists --bvn hoved_organisation --description Hovedorganisation
python med_import.py cli ensure-facet-exists --bvn faglig_organisation --description "Faglig organisation"

python med_import.py cli ensure-class-exists --bvn lo --title "LO" --facet-bvn hoved_organisation
python med_import.py cli ensure-class-exists --bvn metal --title "Dansk Metal" --facet-bvn faglig_organisation --parent-bvn lo
python med_import.py cli ensure-class-exists --bvn 3f --title "3F" --facet-bvn faglig_organisation --parent-bvn lo
