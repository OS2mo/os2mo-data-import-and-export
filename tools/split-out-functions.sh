#!/bin/bash

JOBRUNNER=tools/job-runner.sh

IMPORT_FUNCTIONS=$(cat ${JOBRUNNER} | grep -o "imports_.*()" | cut -f1 -d'(')
EXPORT_FUNCTIONS=$(cat ${JOBRUNNER} | grep -o "exports_.*()" | cut -f1 -d'(')

IMPORT_OUTPUT=tools/job-runner.d/0080-
EXPORT_OUTPUT=tools/job-runner.d/0090-

for i in ${IMPORT_FUNCTIONS}; do
    # Find start and end line numbers for the function
    START_LINE=$(grep -n -m1 "^$i" ${JOBRUNNER} | cut -f1 -d':')
    LENGTH=$(tail -n +$START_LINE ${JOBRUNNER} | grep -n -m1 "^}" | cut -f1 -d':')
    END_LINE=$((START_LINE + LENGTH))
    echo "$i $START_LINE + $LENGTH = $END_LINE"
    # Create our new job-runner.d file
    FILENAME="${IMPORT_OUTPUT}$i.sh"
    sed -n "${START_LINE},${END_LINE}p" ${JOBRUNNER} > ${FILENAME}
    # Delete the function from the source file
    sed -e "${START_LINE},${END_LINE}d" -i ${JOBRUNNER}
done

for i in ${EXPORT_FUNCTIONS}; do
    # Find start and end line numbers for the function
    START_LINE=$(grep -n -m1 "^$i" ${JOBRUNNER} | cut -f1 -d':')
    LENGTH=$(tail -n +$START_LINE ${JOBRUNNER} | grep -n -m1 "^}" | cut -f1 -d':')
    END_LINE=$((START_LINE + LENGTH))
    echo "$i $START_LINE + $LENGTH = $END_LINE"
    # Create our new job-runner.d file
    FILENAME="${EXPORT_OUTPUT}$i.sh"
    sed -n "${START_LINE},${END_LINE}p" ${JOBRUNNER} > ${FILENAME}
    # Delete the function from the source file
    sed -e "${START_LINE},${END_LINE}d" -i ${JOBRUNNER}
done
