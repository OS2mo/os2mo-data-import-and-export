#!/bin/bash
## Script til at rette den forkerte opsætning der blev lavet på Viborgs MED-organisation i test

MOX_URL="${MOX_URL:-http://localhost:8080}"

CLI="venv/bin/python os2mo_data_import/med/mox_util.py cli --mox-base ${MOX_URL}"

${CLI} ensure-class-value --bvn "HO-MED" --variable brugervendtnoegle --new_value "HOV-MED"
${CLI} ensure-class-value --bvn "FÆ-MED" --variable brugervendtnoegle --new_value "FÆL-MED"
${CLI} ensure-class-value --bvn "FA-MED" --variable brugervendtnoegle --new_value "FAG-MED"
${CLI} ensure-class-value --bvn "OM-MED" --variable brugervendtnoegle --new_value "OMR-MED"
${CLI} ensure-class-value --bvn "LO-MED" --variable brugervendtnoegle --new_value "LOK-MED"
${CLI} ensure-class-value --bvn "PE-MED" --variable brugervendtnoegle --new_value "PER-MED"
