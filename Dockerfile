FROM python:3

RUN apt-get update && apt-get -y install  \
    unixodbc-dev \ 
    freetds-dev \
    unixodbc \
    tdsodbc \
    libkrb5-dev \
    libmariadb-dev \
    jq


ADD . /app
WORKDIR /app

RUN pip install -r /app/os2mo_data_import/med/requirements.txt

CMD ["/bin/bash", "/app/os2mo_data_import/med/seed.sh"]
