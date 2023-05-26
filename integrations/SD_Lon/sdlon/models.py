from enum import Enum
from typing import Optional
from uuid import UUID

from pydantic import BaseModel


class JobFunction(str, Enum):
    job_position_identifier = "JobPositionIdentifier"
    employment_name = "EmploymentName"


# TODO: replace this model with the one present in the new SD client
class SDBasePerson(BaseModel):
    cpr: str
    given_name: Optional[str]
    surname: Optional[str]
    telephone_number_identifiers: list[str] = []


class MOBasePerson(BaseModel):
    cpr: str
    givenname: str
    surname: str
    name: str
    uuid: UUID
