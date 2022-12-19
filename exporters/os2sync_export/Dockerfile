# SPDX-FileCopyrightText: Magenta ApS
# SPDX-License-Identifier: MPL-2.0

FROM python:3.8
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update && apt-get -y install unixodbc-dev freetds-dev unixodbc tdsodbc libkrb5-dev libmariadb-dev

WORKDIR /app

ENV POETRY_HOME=/opt/poetry \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_VERSION=1.3.1

RUN curl -sSL https://install.python-poetry.org | python3 - 
COPY pyproject.toml poetry.lock ./

RUN POETRY_NO_INTERACTION=1 /opt/poetry/bin/poetry install --no-root --only main

COPY ./ ./

CMD ["uvicorn", "os2sync_export.main:app", "--host", "0.0.0.0"]
