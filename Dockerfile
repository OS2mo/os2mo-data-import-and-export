FROM python:3

RUN apt-get update && apt-get -y install  \
    unixodbc-dev \ 
    freetds-dev \
    unixodbc \
    tdsodbc \
    libkrb5-dev \
    libmariadb-dev \
    jq


COPY ./os2mo_data_import/med/requirements.txt app/os2mo_data_import/med/requirements.txt
RUN pip install -r /app/os2mo_data_import/med/requirements.txt

COPY . /app
WORKDIR /app

CMD ["/bin/bash", "/app/os2mo_data_import/med/seed.sh"]
