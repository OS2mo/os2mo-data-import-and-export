# SPDX-FileCopyrightText: Magenta ApS
# SPDX-License-Identifier: MPL-2.0

FROM python:3.11
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update && apt-get -y install unixodbc-dev freetds-dev unixodbc tdsodbc libkrb5-dev libmariadb-dev

# These have to be installed manually for debugging everytime a Kubernetes pod
# is recreated which is very annoying. Let's include them for now until the
# SD code becomes more stable
RUN apt-get install -y sqlite3 screen

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    POETRY_HOME=/opt/poetry \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_VERSION=1.3.1

RUN curl -sSL https://install.python-poetry.org | python3 -
COPY pyproject.toml poetry.lock ./

RUN POETRY_NO_INTERACTION=1 /opt/poetry/bin/poetry install --no-root --no-dev

COPY . ./

CMD ["uvicorn", "sdlon.main:app", "--host", "0.0.0.0"]
