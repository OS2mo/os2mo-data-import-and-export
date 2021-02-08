FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8

# Install mora_helpers
WORKDIR /srv/
RUN git clone https://github.com/OS2mo/os2mo-data-import-and-export
RUN pip install -e os2mo-data-import-and-export/os2mo_data_import
RUN pip install more_itertools

COPY ./requirements.txt /srv/requirements.txt
RUN pip install -r /srv/requirements.txt

WORKDIR /app
COPY ./app /app
