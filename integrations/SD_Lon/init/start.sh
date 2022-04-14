#!/bin/bash

SD_IMPORT_RUN_DB=${SD_IMPORT_RUN_DB:-/mnt/dipex/run_db.sqlite}

if [ ! -f ${SD_IMPORT_RUN_DB} ]; then
    echo "${SD_IMPORT_RUN_DB} not found, creating!"
    sqlite3 ${SD_IMPORT_RUN_DB} "CREATE TABLE runs (id INTEGER PRIMARY KEY, from_date timestamp, to_date timestamp, status text);"
    sqlite3 ${SD_IMPORT_RUN_DB} "INSERT INTO runs VALUES (1, \"${SD_GLOBAL_FROM_DATE}\", \"${SD_GLOBAL_FROM_DATE}\", \"Initializing RunDB\")"
fi
