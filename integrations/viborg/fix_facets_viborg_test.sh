#!/bin/bash
## Script til at rette den forkerte opsætning der blev lavet på Viborgs MED-organisation i test

MOX_URL="${MOX_URL:-http://localhost:8080}"

CLI="venv/bin/python os2mo_data_import/med/mox_util.py cli --mox-base ${MOX_URL}"

${CLI} ensure-class-value --bvn "Hoved-MED" --variable titel --new_value "Hoved-MED"
${CLI} ensure-class-value --bvn "Hoved-MED" --variable brugervendtnoegle --new_value "HO-MED"

${CLI} ensure-class-value --bvn "Fælles-MED" --variable titel --new_value "Fælles-MED"
${CLI} ensure-class-value --bvn "Fælles-MED" --variable brugervendtnoegle --new_value "FÆ-MED"

${CLI} ensure-class-value --bvn "Fag-MED" --variable titel --new_value "Fag-MED"
${CLI} ensure-class-value --bvn "Fag-MED" --variable brugervendtnoegle --new_value "FA-MED"

${CLI} ensure-class-value --bvn "Område-MED" --variable titel --new_value "Område-MED"
${CLI} ensure-class-value --bvn "Område-MED" --variable brugervendtnoegle --new_value "OM-MED"

${CLI} ensure-class-value --bvn "Lokal-MED" --variable titel --new_value "Lokal-MED"
${CLI} ensure-class-value --bvn "Lokal-MED" --variable brugervendtnoegle --new_value "LO-MED"

${CLI} ensure-class-value --bvn "Personalemøder med MED-status" --variable titel --new_value "Personalemøder med MED-status"
${CLI} ensure-class-value --bvn "Personalemøder med MED-status" --variable brugervendtnoegle --new_value "PE-MED"
