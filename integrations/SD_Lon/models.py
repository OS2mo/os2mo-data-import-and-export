from typing import Optional

from pydantic import BaseModel


class SDBasePerson(BaseModel):
    cpr: str
    given_name: Optional[str]
    surname: Optional[str]
