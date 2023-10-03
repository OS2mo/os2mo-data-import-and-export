# SPDX-FileCopyrightText: Magenta ApS
# SPDX-License-Identifier: MPL-2.0
import os

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def get_db_url() -> str:
    return f"postgresql+psycopg2://{os.environ['APP_DBUSER']}:{os.environ['APP_DBPASSWORD']}@{os.environ['PGHOST']}/{os.environ['APP_DATABASE']}"


def get_engine() -> Engine:
    return create_engine(get_db_url())
