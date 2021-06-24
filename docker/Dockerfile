FROM python:3

RUN apt-get update && apt-get -y install  unixodbc-dev  freetds-dev  unixodbc  tdsodbc  libkrb5-dev  libmariadb-dev 

ENV SUPERCRONIC_URL=https://github.com/aptible/supercronic/releases/download/v0.1.12/supercronic-linux-amd64 \
    SUPERCRONIC=supercronic-linux-amd64 \
    SUPERCRONIC_SHA1SUM=048b95b48b708983effb2e5c935a1ef8483d9e3e

RUN curl -fsSLO "$SUPERCRONIC_URL" \
    && echo "${SUPERCRONIC_SHA1SUM}  ${SUPERCRONIC}" | sha1sum -c - \
    && chmod +x "$SUPERCRONIC" \
    && mv "$SUPERCRONIC" "/usr/local/bin/${SUPERCRONIC}" \
    && ln -s "/usr/local/bin/${SUPERCRONIC}" /usr/local/bin/supercronic


COPY . /code
WORKDIR /code

RUN find . -name 'requirements.txt' | grep -v venv/ | xargs -l1 pip install -r
RUN pip install -r ./integrations/requirements/common.txt
RUN pip install -r ./integrations/requirements/test.txt
RUN pip install ./os2mo_data_import --upgrade

ENV PYTHONUNBUFFERED=1 \
    CUSTOMER_SETTINGS=/code/settings/settings.json \
    PYTHONPATH="${PYTHONPATH}:/code"

CMD ["supercronic", "-debug", "/code/crontab"]