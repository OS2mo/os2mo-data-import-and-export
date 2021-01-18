
MOX_URL="${MOX_URL:-http://localhost:8080}"

CLI="venv/bin/python os2mo_data_import/med/mox_util.py cli --mox-base ${MOX_URL}"

# Organisation Unit Types
#------------------------
${CLI} ensure-class-exists --bvn "Prisme" --title "Prisme" --facet-bvn "org_unit_type"