FROM python:3.11

RUN apt-get update && apt-get -y install unixodbc-dev freetds-dev unixodbc tdsodbc libkrb5-dev libmariadb-dev

# A few convenience packages
RUN apt-get -y install vim jq less screen

# Enviroment variables for poetry
ENV PIP_DISABLE_PIP_VERSION_CHECK=on \
    PYTHONPATH=/app:/app/backend \
    POETRY_VERSION="1.3.1" \
    POETRY_VIRTUALENVS_CREATE=false \
    PYTHONUNBUFFERED=1 \
    CUSTOMER_SETTINGS=/code/settings/settings.json \
    PYTHONPATH="${PYTHONPATH}:/code"

# Install requirements
RUN pip3 install --no-cache-dir poetry==${POETRY_VERSION}
WORKDIR /code
COPY pyproject.toml poetry.lock ./

RUN poetry install --no-root --only main --no-interaction
COPY . .
CMD ["python", "./metacli.py"]
