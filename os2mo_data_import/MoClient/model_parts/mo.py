from typing import Optional

from pydantic import Field

from os2mo_data_import.MoClient.model_parts.interface import ConfiguredBase


class Validity(ConfiguredBase):
    from_date: str = Field("1930-01-01", alias="from")
    to_date: Optional[str] = Field(None, alias="to")


class Parent(ConfiguredBase):
    uuid: str


class OrgUnitHierarchy(ConfiguredBase):
    uuid: str


class OrgUnitType(ConfiguredBase):
    uuid: str


class OrgUnitLevel(ConfiguredBase):
    uuid: str


class Person(ConfiguredBase):
    uuid: str


class OrgUnitRef(ConfiguredBase):
    uuid: str


class JobFunction(ConfiguredBase):
    uuid: str


class Primary(ConfiguredBase):
    uuid: str


class EngagementType(ConfiguredBase):
    uuid: str


class OrganisationRef(ConfiguredBase):
    uuid: str


class EngagementRef(ConfiguredBase):
    uuid: str


class Visibility(ConfiguredBase):
    uuid: str


class AddressType(ConfiguredBase):
    uuid: str


class Responsibility(ConfiguredBase):
    uuid: str


class ManagerLevel(ConfiguredBase):
    uuid: str


class ManagerType(ConfiguredBase):
    uuid: str


class EngagementAssociationType(ConfiguredBase):
    uuid: str
