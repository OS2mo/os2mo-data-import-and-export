from enum import Enum
from typing import Optional
from uuid import UUID

from pydantic import BaseModel


class JobFunction(str, Enum):
    job_position_identifier = "JobPositionIdentifier"
    employment_name = "EmploymentName"


# TODO: replace these models with the one present in the new SD client

# TODO: rename
class EmploymentWithTelephoneNumberIdentifier(BaseModel):
    employment_identifier: str
    telephone_number_ids: list[str] = []


class SDBasePerson(BaseModel):
    cpr: str
    given_name: Optional[str]
    surname: Optional[str]
    emp_with_telephone_number_identifiers: list[
        EmploymentWithTelephoneNumberIdentifier
    ] = []


class MOBasePerson(BaseModel):
    cpr: str
    givenname: str
    surname: str
    name: str
    uuid: UUID


class ITUserSystem(BaseModel):
    # UUID of the IT-system itself
    uuid: UUID
    # User key of the IT-user
    user_key: str
