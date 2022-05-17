from enum import Enum
import logging
from typing import Optional

from pydantic import BaseModel


class JobFunction(str, Enum):
    job_position_identifier = "JobPositionIdentifier"
    employment_name = "EmploymentName"


class LogLevel(str, Enum):
    debug = logging.getLevelName(logging.DEBUG)
    info = logging.getLevelName(logging.INFO)
    warning = logging.getLevelName(logging.WARNING)
    error = logging.getLevelName(logging.ERROR)
    critical = logging.getLevelName(logging.CRITICAL)


class SDBasePerson(BaseModel):
    cpr: str
    given_name: Optional[str]
    surname: Optional[str]
