# SPDX-FileCopyrightText: Magenta ApS
# SPDX-License-Identifier: MPL-2.0
from uuid import UUID

from sqlalchemy.orm import sessionmaker

from .engine import get_engine
from .models import Payload


Session = sessionmaker()


def log_payload(
    request_uuid: UUID,
    full_url: str,
    params: str,
    response: str,
    status_code: int,
) -> None:
    """Log a given SD payload to the payload database"""
    Session.configure(bind=get_engine())
    session = Session()
    payload = Payload(
        id=request_uuid,
        full_url=full_url,
        params=params,
        response=response,
        status_code=status_code,
    )
    session.add(payload)
    session.commit()
