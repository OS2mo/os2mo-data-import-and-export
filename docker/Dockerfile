FROM python:3

RUN apt-get update && \
    apt-get -y install --no-install-recommends \
    freetds-dev \
    libkrb5-dev \
    libmariadb-dev \
    tdsodbc \
    unixodbc \
    unixodbc-dev \
    && apt-get clean

COPY . /code
WORKDIR /code
COPY ./reports/requirements ./reports/requirements
COPY ./integrations/requirements ./integrations/requirements
COPY ./integrations/calculate_primary/requirements ./integrations/calculate_primary/requirements
COPY ./exporters/sql_export/requirements.txt ./exporters/sql_export/requirements.txt
COPY ./exporters/sql_export/tests/requirements.txt ./exporters/sql_export/tests/requirements.txt
COPY ./exporters/utils/requirements.txt ./exporters/utils/requirements.txt
COPY ./exporters/requirements.txt ./exporters/requirements.txt
COPY ./exporters/os2rollekatalog/requirements.txt ./exporters/os2rollekatalog/requirements.txt
COPY ./exporters/os2phonebook/requirements.txt ./exporters/os2phonebook/requirements.txt
COPY ./exporters/emus/requirements.txt ./exporters/emus/requirements.txt
COPY ./integrations/calculate_primary/requirements.txt ./integrations/calculate_primary/requirements.txt
COPY ./integrations/requirements.txt ./integrations/requirements.txt
COPY ./integrations/SD_Lon/requirements.txt ./integrations/SD_Lon/requirements.txt
COPY ./integrations/opus/org_tree_print/requirements.txt ./integrations/opus/org_tree_print/requirements.txt
COPY ./integrations/dar_helper/requirements.txt ./integrations/dar_helper/requirements.txt
COPY ./requirements.txt ./requirements.txt
COPY ./os2mo_data_import/os2mo_helpers/requirements.txt ./os2mo_data_import/os2mo_helpers/requirements.txt
COPY ./os2mo_data_import/requirements.txt ./os2mo_data_import/requirements.txt
COPY ./os2mo_data_import/mox_helpers/requirements.txt ./os2mo_data_import/mox_helpers/requirements.txt
COPY ./tools/requirements.txt ./tools/requirements.txt
COPY ./tools/check_connectivity/requirements.txt ./tools/check_connectivity/requirements.txt

RUN pip install -r requirements.txt

COPY ./os2mo_data_import ./os2mo_data_import
RUN pip install -e ./os2mo_data_import


ENV PYTHONUNBUFFERED=1 \
    CUSTOMER_SETTINGS=/code/settings/settings.json \
    PYTHONPATH="${PYTHONPATH}:/code"

COPY . .

CMD ["python", "./metacli.py"]
