# SPDX-FileCopyrightText: Magenta ApS
# SPDX-License-Identifier: MPL-2.0

FROM python:3.10
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update && apt-get -y install unixodbc-dev freetds-dev unixodbc tdsodbc libkrb5-dev libmariadb-dev

WORKDIR /app

ENV POETRY_HOME=/opt/poetry \
    POETRY_VIRTUALENVS_CREATE=false

RUN curl -sSL https://install.python-poetry.org | python3 -
COPY pyproject.toml poetry.lock ./

RUN POETRY_NO_INTERACTION=1 /opt/poetry/bin/poetry install --no-root --no-dev

COPY ./ ./

CMD ["uvicorn", "os2sync_export.main:app", "--host", "0.0.0.0"]
