from enum import Enum
from typing import Optional

from pydantic import BaseModel


class JobFunction(str, Enum):
    job_position_identifier = "JobPositionIdentifier"
    employment_name = "EmploymentName"


class LogLevel(str, Enum):
    debug = "DEBUG"
    info = "INFO"
    warning = "WARNING"
    error = "ERROR"
    critical = "CRITICAL"


class SDBasePerson(BaseModel):
    cpr: str
    given_name: Optional[str]
    surname: Optional[str]
