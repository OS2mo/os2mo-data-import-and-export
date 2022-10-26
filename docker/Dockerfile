FROM python:3.9

RUN apt-get update && apt-get -y install unixodbc-dev freetds-dev unixodbc tdsodbc libkrb5-dev libmariadb-dev

# A few convenience packages
RUN apt-get -y install vim jq less screen

# Enviroment variables for poetry
ENV PIP_DISABLE_PIP_VERSION_CHECK=on \
    PYTHONPATH=/app:/app/backend \
    POETRY_VERSION="1.1.8" \
    POETRY_VIRTUALENVS_CREATE=false \
    PYTHONUNBUFFERED=1 \
    CUSTOMER_SETTINGS=/code/settings/settings.json \
    PYTHONPATH="${PYTHONPATH}:/code"

# Install requirements
RUN pip3 install --no-cache-dir poetry==${POETRY_VERSION}
COPY pyproject.toml poetry.lock ./
RUN poetry install --no-interaction

WORKDIR /code
COPY . .

CMD ["python", "./metacli.py"]
