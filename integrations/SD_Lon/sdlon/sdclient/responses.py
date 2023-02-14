from datetime import date
from typing import List
from typing import Optional
from uuid import UUID

from pydantic import BaseModel


class DefaultDates(BaseModel):
    ActivationDate: date
    DeactivationDate: date

class Department(DefaultDates):
    DepartmentIdentifier: str
    DepartmentLevelIdentifier: str
    DepartmentName: Optional[str]
    DepartmentUUIDIdentifier: Optional[UUID]


class EmploymentStatus(DefaultDates):
    # TODO: add constraint
    EmploymentStatusCode: str


class Employment(BaseModel):
    # TODO: add missing fields
    EmploymentIdentifier: str
    EmploymentDate: date
    AnniversaryDate: date
    EmploymentStatus: EmploymentStatus


class Person(BaseModel):
    """
    An SD (GetEmployment) person... can maybe be generalized
    """
    # TODO: add constraint
    PersonCivilRegistrationIdentifier: str
    Employment: List[Employment]


class GetDepartmentResponse(BaseModel):
    """
    Response model for SDs GetDepartment20111201
    """
    # TODO: add missing fields
    RegionIdentifier: str
    RegionUUIDIdentifier: Optional[UUID]
    InstitutionIdentifier: str
    InstitutionUUIDIdentifier: Optional[UUID]
    Department: List[Department]


class GetEmploymentResponse(BaseModel):
    """
    Response model for SDs GetDepartment20111201
    """
    Person: List[Person]
