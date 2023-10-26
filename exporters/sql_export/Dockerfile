# SPDX-FileCopyrightText: Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
FROM python:3.11-bullseye
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Install binary database connection libraries
RUN curl https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc \
 && curl https://packages.microsoft.com/config/debian/11/prod.list | tee /etc/apt/sources.list.d/microsoft.list \
 && apt-get update \
 && apt-get --no-install-recommends -y install unixodbc-dev freetds-dev unixodbc tdsodbc libkrb5-dev libmariadb-dev sqlcmd \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    POETRY_VERSION="1.3.1" \
    POETRY_HOME=/opt/poetry

RUN curl -sSL https://install.python-poetry.org | python3 - 

WORKDIR /opt/sql_export
COPY poetry.lock pyproject.toml ./
RUN POETRY_NO_INTERACTION=1 /opt/poetry/bin/poetry  install --no-root --only=main

COPY sql_export ./sql_export/
CMD [ "uvicorn", "--factory", "sql_export.main:create_app", "--host", "0.0.0.0" ]

# Add build version to the environment last to avoid build cache misses
ARG COMMIT_TAG
ARG COMMIT_SHA
ENV COMMIT_TAG=${COMMIT_TAG:-HEAD} \
    COMMIT_SHA=${COMMIT_SHA}
