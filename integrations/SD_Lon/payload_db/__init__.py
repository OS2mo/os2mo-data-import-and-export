# SPDX-FileCopyrightText: Magenta ApS
# SPDX-License-Identifier: MPL-2.0
from sqlalchemy.orm import sessionmaker

from .engine import get_engine
from .models import Payload


Session = sessionmaker()


def log_payload(full_url: str, params: str, response: str):
    Session.configure(bind=get_engine())
    session = Session()
    payload = Payload(full_url=full_url, params=params, response=response)
    session.add(payload)
    session.commit()
