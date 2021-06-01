FROM python:3.8-slim
RUN apt-get update && apt-get -y install unixodbc-dev  freetds-dev  unixodbc  tdsodbc  libkrb5-dev  libmariadb-dev  && rm -rf /var/lib/apt/lists/*

COPY integrations/requirements/common.txt . 
COPY os2mo_data_import os2mo_data_import
RUN pip install -r common.txt && pip install os2mo_data_import/.
RUN groupadd -r dipex && useradd --no-log-init -r -g dipex sys_magenta_dipex
USER sys_magenta_dipex 
WORKDIR /code
COPY . /code

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH="${PYTHONPATH}:/code/."