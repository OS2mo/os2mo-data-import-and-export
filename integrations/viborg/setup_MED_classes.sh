#!/bin/bash
## Script til opsætning til Viborgs MED-organisation

MOX_URL="${MOX_URL:-http://localhost:8080}"

CLI="venv/bin/python os2mo_data_import/med/mox_util.py cli --mox-base ${MOX_URL}"

# Organisation Unit Types
#------------------------
${CLI} ensure-class-exists --bvn "HOV-MED" --title "Hoved-MED" --facet-bvn "org_unit_type"
${CLI} ensure-class-exists --bvn "FÆL-MED" --title "Fælles-MED" --facet-bvn "org_unit_type"
${CLI} ensure-class-exists --bvn "FAG-MED" --title "Fag-MED" --facet-bvn "org_unit_type"
${CLI} ensure-class-exists --bvn "OMR-MED" --title "Område-MED" --facet-bvn "org_unit_type"
${CLI} ensure-class-exists --bvn "LOK-MED" --title "Lokal-MED" --facet-bvn "org_unit_type"
${CLI} ensure-class-exists --bvn "PER-MED" --title "Personalemøder med MED-status" --facet-bvn "org_unit_type"

# Association Types
#------------------
${CLI} ensure-class-exists --bvn "MED-Tillidsrepræsentant" --title "MED-Tillidsrepræsentant" --facet-bvn "association_type"
${CLI} ensure-class-exists --bvn "MED-Arbejdsmiljørepræsentant" --title "MED-Arbejdsmiljørepræsentant" --facet-bvn "association_type"
${CLI} ensure-class-exists --bvn "MED-Næstformand" --title "MED-Næstformand" --facet-bvn "association_type"
${CLI} ensure-class-exists --bvn "MED-Formand" --title "MED-Formand" --facet-bvn "association_type"
${CLI} ensure-class-exists --bvn "MED-Medlem" --title "MED-Medlem" --facet-bvn "association_type"
${CLI} ensure-class-exists --bvn "MED-Medlem-suppleant" --title "MED-Medlem-suppleant" --facet-bvn "association_type"

# org_unit_level
#---------------
${CLI} ensure-class-exists --bvn "MED-enhed" --title "MED-enhed" --facet-bvn "org_unit_level"
