FROM python:3

RUN apt-get update && apt-get -y install  unixodbc-dev  freetds-dev  unixodbc  tdsodbc  libkrb5-dev  libmariadb-dev 


COPY . /app
WORKDIR /app

RUN find . -name 'requirements.*' | grep -v venv/ | xargs -l1 pip install -r
RUN pip install -r ./integrations/requirements/common.txt
RUN pip install -r ./integrations/requirements/test.txt
RUN pip install ./os2mo_data_import --upgrade

ENV PYTHONUNBUFFERED=1 \
    CUSTOMER_SETTINGS=/app/settings/settings.json \
    PYTHONPATH="${PYTHONPATH}:/app"