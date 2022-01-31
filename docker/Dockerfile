FROM python:3.9

RUN apt-get update && apt-get -y install unixodbc-dev freetds-dev unixodbc tdsodbc libkrb5-dev libmariadb-dev

# A few convenience packages
RUN apt-get -y install vim jq less screen

# Enviroment variables for poetry
ENV PIP_DISABLE_PIP_VERSION_CHECK=on \
    PYTHONPATH=/app:/app/backend \
    POETRY_VERSION="1.1.8" \
    POETRY_VIRTUALENVS_CREATE=false

# Install requirements
RUN pip3 install --no-cache-dir poetry==${POETRY_VERSION}
COPY poetry.lock pyproject.toml /code/
WORKDIR /code
RUN poetry install --no-interaction

COPY ./os2mo_data_import ./os2mo_data_import
RUN pip install -e ./os2mo_data_import


ENV PYTHONUNBUFFERED=1 \
    CUSTOMER_SETTINGS=/code/settings/settings.json \
    PYTHONPATH="${PYTHONPATH}:/code"

COPY . .

CMD ["python", "./metacli.py"]
