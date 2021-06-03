FROM python:3.8-slim
RUN apt-get update &&  apt-get -y install \
    unixodbc-dev \
    freetds-dev \
    unixodbc \
    tdsodbc \
    libkrb5-dev \
    libmariadb-dev \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

ENV SUPERCRONIC_URL=https://github.com/aptible/supercronic/releases/download/v0.1.12/supercronic-linux-amd64 \
    SUPERCRONIC=supercronic-linux-amd64 \
    SUPERCRONIC_SHA1SUM=048b95b48b708983effb2e5c935a1ef8483d9e3e

RUN curl -fsSLO "$SUPERCRONIC_URL" \
    && echo "${SUPERCRONIC_SHA1SUM}  ${SUPERCRONIC}" | sha1sum -c - \
    && chmod +x "$SUPERCRONIC" \
    && mv "$SUPERCRONIC" "/usr/local/bin/${SUPERCRONIC}" \
    && ln -s "/usr/local/bin/${SUPERCRONIC}" /usr/local/bin/supercronic


# RUN groupadd -r dipex && useradd --no-log-init -r -g dipex sys_magenta_dipex
# USER sys_magenta_dipex 
WORKDIR /code
COPY . /code
RUN python -m venv venv && . /code/venv/bin/activate && pip install -r integrations/requirements/common.txt 
RUN  pip install /code/os2mo_data_import/.

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH="${PYTHONPATH}:"

CMD ["supercronic", "-debug", "/code/crontab"]