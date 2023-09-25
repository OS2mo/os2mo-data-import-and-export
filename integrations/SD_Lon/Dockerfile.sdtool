# SPDX-FileCopyrightText: Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0

FROM tiangolo/uvicorn-gunicorn-fastapi:python3.11

ENV POETRY_VERSION="1.3.2"

RUN apt-get update \
 && apt-get -y install --no-install-recommends unixodbc-dev=2.3.11-2+deb12u1 \
    freetds-dev=1.3.17+ds-2 unixodbc=2.3.11-2+deb12u1 tdsodbc=1.3.17+ds-2 \
    libkrb5-dev=1.20.1-2 libmariadb-dev=1:10.11.3-1 \
 && apt-get -y install --no-install-recommends screen=4.9.0-4 vim=2:9.0.1378-2 less=590-2 sqlite3=3.40.1-2 \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/
RUN git clone -b 4.54.6 https://github.com/OS2mo/os2mo-data-import-and-export \
 && pip3 install --no-cache-dir poetry==${POETRY_VERSION}

WORKDIR /opt/os2mo-data-import-and-export/integrations/SD_Lon
RUN poetry install --no-interaction --no-root --no-dev

COPY ./requirements.txt /app/requirements.txt
COPY ./requirements /app/requirements
RUN pip3 install --no-cache-dir -r /app/requirements.txt

# These are not used but have to be there... don't worry about it - it's just DIPEX...
ENV SD_GLOBAL_FROM_DATE=2000-01-01
ENV SD_IMPORT_RUN_DB=/not/used
ENV SD_JOB_FUNCTION=EmploymentName
ENV SD_MONTHLY_HOURLY_DIVIDE=1

ENV TZ="Europe/Copenhagen"

WORKDIR /app
COPY ./app /app
