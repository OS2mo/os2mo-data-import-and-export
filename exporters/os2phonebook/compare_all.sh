#!/bin/bash
# Script to validate this exporter against the original importer from os2phonebook
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR" || exit

# We need a JQ version with 'walk' (>=1.6)
# $ wget https://github.com/stedolan/jq/releases/download/jq-1.6/jq-linux64
# $ chmod +x jq-linux64
JQ=./jq-linux64

# We do not care about the order of entries in lists
SORT_ALL='walk(if type == "array" then sort else . end)'

# Cache folder for old importer
CACHE_FOLDER=../../../os2phonebook/dev-environment/cache/

# Employees
#----------
echo "Comparing employees"
GENERATED_EMPLOYEES=../../tmp/employees.json
TRUTH_EMPLOYEES=$CACHE_FOLDER/map_employees.json
diff <($JQ -S "$SORT_ALL" $TRUTH_EMPLOYEES) <($JQ -S "$SORT_ALL" $GENERATED_EMPLOYEES)
diff <($JQ -S "$SORT_ALL" $TRUTH_EMPLOYEES) <($JQ -S "$SORT_ALL" $GENERATED_EMPLOYEES) | wc -l

# Org Units
#----------
echo "Comparing org_units"
GENERATED_ORG_UNITS=../../tmp/org_units.json
# Path to file generated with the old exporter
TRUTH_ORG_UNITS=$CACHE_FOLDER/map_org_units.json
diff <($JQ -S "$SORT_ALL" $TRUTH_ORG_UNITS) <($JQ -S "$SORT_ALL" $GENERATED_ORG_UNITS)
diff <($JQ -S "$SORT_ALL" $TRUTH_ORG_UNITS) <($JQ -S "$SORT_ALL" $GENERATED_ORG_UNITS) | wc -l
