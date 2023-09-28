#!/bin/bash
# SPDX-FileCopyrightText: Magenta ApS
# SPDX-License-Identifier: MPL-2.0

set -o nounset
set -o errexit
set -o pipefail
set -o xtrace

# Create DB user $APP_DBUSER if it does not already exist
if [ "$( psql -XtAc "SELECT 1 FROM pg_roles WHERE rolname = '$APP_DBUSER'" )" = '1' ]
then
    echo "Database user $APP_DBUSER already exists, continuing ..."
else
    echo "Database $APP_DBUSER does not exist, creating ..."
    psql -XtAc "CREATE USER $APP_DBUSER LOGIN PASSWORD '$APP_DBPASSWORD'"
fi

# Create database $APP_DATABASE if it does not already exist
if [ "$( psql -XtAc "SELECT 1 FROM pg_database WHERE datname = '$APP_DATABASE'" )" = '1' ]
then
    echo "Database $APP_DATABASE already exists, continuing ..."
else
    echo "Database $APP_DATABASE does not exist, creating"
    psql -XtAc "CREATE DATABASE $APP_DATABASE OWNER $APP_DBUSER"
fi

# Apply Alembic migrations
alembic upgrade head

# Run app
uvicorn --factory sdlon.main:create_app --host 0.0.0.0

# docker-compose.yaml used to invoke the app like this:
# uvicorn sdlon.main:app --host 0.0.0.0 --reload
# Seems to be for local development?
