from typing import Optional
from uuid import UUID

from pydantic import Field

from os2mo_data_import.MoClient.model_parts.interface import ConfiguredBase


class Validity(ConfiguredBase):
    from_date: str = Field("1930-01-01", alias="from")
    to_date: Optional[str] = Field(None, alias="to")


class Parent(ConfiguredBase):
    uuid: UUID


class OrgUnitHierarchy(ConfiguredBase):
    uuid: UUID


class OrgUnitType(ConfiguredBase):
    uuid: UUID


class OrgUnitLevel(ConfiguredBase):
    uuid: UUID


class Person(ConfiguredBase):
    uuid: UUID


class OrgUnitRef(ConfiguredBase):
    uuid: UUID


class JobFunction(ConfiguredBase):
    uuid: UUID


class Primary(ConfiguredBase):
    uuid: UUID


class EngagementType(ConfiguredBase):
    uuid: UUID


class OrganisationRef(ConfiguredBase):
    uuid: UUID


class EngagementRef(ConfiguredBase):
    uuid: UUID


class Visibility(ConfiguredBase):
    uuid: UUID


class AddressType(ConfiguredBase):
    uuid: UUID


class Responsibility(ConfiguredBase):
    uuid: UUID


class ManagerLevel(ConfiguredBase):
    uuid: UUID


class ManagerType(ConfiguredBase):
    uuid: UUID


class EngagementAssociationType(ConfiguredBase):
    uuid: UUID
