FROM python:3.8

RUN apt-get update && apt-get -y install  unixodbc-dev  freetds-dev  unixodbc  tdsodbc  libkrb5-dev  libmariadb-dev

COPY integrations/gir/docker/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r  /requirements.txt

WORKDIR /app
COPY integrations/gir/initial_read integrations/gir/initial_read
COPY exporters/utils exporters/utils


ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH="${PYTHONPATH}:/app"

ENTRYPOINT ["python", "integrations/gir/initial_read/main.py"]