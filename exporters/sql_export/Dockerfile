# SPDX-FileCopyrightText: Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
FROM python:3.11

# Install binary database connection libraries
RUN apt-get update \
 && apt-get --no-install-recommends -y install software-properties-common unixodbc-dev freetds-dev unixodbc tdsodbc libkrb5-dev libmariadb-dev \
 && curl https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc \
 && add-apt-repository --yes "$(wget -qO- https://packages.microsoft.com/config/debian/11/prod.list)" \
 # I have no idea why this needs to be run twice.
 && add-apt-repository --yes "$(wget -qO- https://packages.microsoft.com/config/debian/11/prod.list)" \
 && apt-get update \
 && apt-get --no-install-recommends -y install sqlcmd \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    POETRY_VERSION="1.3.1" \
    POETRY_HOME=/opt/poetry \
    VIRTUAL_ENV="/venv"
ENV PATH="$VIRTUAL_ENV/bin:$POETRY_HOME/bin:$PATH"

# Install poetry in an isolated environment
RUN python -m venv $POETRY_HOME \
    && pip install --no-cache-dir poetry==${POETRY_VERSION}

# Install project in another isolated environment
WORKDIR /opt
RUN python -m venv $VIRTUAL_ENV
COPY poetry.lock pyproject.toml ./
RUN poetry install --no-root --only=main

COPY . /opt/sql_export
CMD [ "uvicorn", "--factory", "sql_export.main:create_app", "--host", "0.0.0.0" ]

# Add build version to the environment last to avoid build cache misses
ARG COMMIT_TAG
ARG COMMIT_SHA
ENV COMMIT_TAG=${COMMIT_TAG:-HEAD} \
    COMMIT_SHA=${COMMIT_SHA}
