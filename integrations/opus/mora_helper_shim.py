from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Any
from typing import Final
from typing import Literal
from typing import NotRequired
from typing import TypedDict
from uuid import UUID

from os2mo_helpers.mora_helpers import MoraHelper


class ItSystemDict(TypedDict):
    name: object
    uuid: object


# TODO: copied from calculate primary
class ClassDict(TypedDict):
    uuid: str
    user_key: str


class AddressType(TypedDict):
    uuid: str


class Address(TypedDict):
    address_type: AddressType
    value: Any
    uuid: str


class OrgUnit(TypedDict):
    uuid: str


class Person(TypedDict):
    uuid: str


class ManagerType(TypedDict):
    uuid: str


class ManagerLevel(TypedDict):
    uuid: str


class Responsibility(TypedDict):
    uuid: str


Validity: Final = TypedDict("Validity", {"from": datetime, "to": datetime | None})


class Visibility(TypedDict):
    uuid: str


class CreateAddressPayload(TypedDict):
    type: Literal["address"]
    value: object
    address_type: object
    validity: Validity
    org_unit: NotRequired[OrgUnit]
    person: NotRequired[Person]


class EditAddressPayload(TypedDict):
    type: Literal["address"]
    data: PerformAddressUpdateArgs


class ManagerFunction(TypedDict):
    user_key: str
    uuid: str
    org_unit: OrgUnit
    person: Person
    manager_type: ManagerType
    manager_level: ManagerLevel
    responsibility: list[Responsibility]
    validity: Validity


class PossiblyAddress(TypedDict):
    value: object
    uuid: object


class PerformAddressUpdateArgs(TypedDict):
    address_type: AddressType
    value: Any
    validity: Validity
    visibility: NotRequired[Visibility]
    user_uuid: NotRequired[Any]
    unit_uuid: NotRequired[str]


class MoObject(TypedDict):
    uuid: str


class CreateOrgUnitPayload(TypedDict):
    uuid: object
    user_key: object
    name: object
    parent: MoObject
    org_unit_type: MoObject
    validity: Validity


class EditOrgUnitPayload(TypedDict):
    type: Literal["org_unit"]
    data: Any


@dataclass
class MoraHelperShim:
    _helper: MoraHelper

    # e/{}/details/address
    # e/{}/details/manager?at=
    def _mo_lookup(self, uuid, url) -> list[Address] | list[ManagerFunction]:
        return self._helper._mo_lookup(uuid, url)

    # details/create
    # details/edit
    # ou/create
    # e/create
    # details/terminate
    def _mo_post(self, url, payload):
        pass

    def read_organisation(self) -> str | UUID:
        raise NotImplementedError

    def read_it_systems(self) -> list[object]:
        raise NotImplementedError

    @lru_cache(maxsize=None)
    def ensure_class_in_facet(
        self,
        facet: str,
        bvn: str,
        title: str | None = None,
        scope: str = "TEXT",
        uuid: UUID | None = None,
        owner: UUID | None = None,
    ) -> UUID:
        raise NotImplementedError

    def read_classes_in_facet(self, facet: str) -> tuple[list[ClassDict], str]:
        raise NotImplementedError

    def read_ou_address(
        self, uuid, scope: Literal[None], return_all: Literal[True]
    ) -> list[dict[str, object]]:
        raise NotImplementedError

    def read_ou(self, uuid) -> dict[object, object]:
        raise NotImplementedError

    def get_e_itsystems(self, uuid, it_system_uuid) -> list[object]:
        raise NotImplementedError

    def read_user(self, user_cpr: str, use_cache=False) -> object:
        raise NotImplementedError

    def read_user_engagement(self, uuid, read_all: Literal[True]) -> list[object]:
        raise NotImplementedError

    def read_user_engagements(self, uuid, read_all: Literal[True]) -> list[object]:
        raise NotImplementedError

    def read_ou_root(self, org) -> object:
        raise NotImplementedError

    # TODO: copied from calculate primary
    # async def read_classes_in_facet(self, facet: str) -> tuple[list[ClassDict], str]:
    #     res = await self._gql_client.get_facet_classes_by_user_key(facet)
    #     classes: list[ClassDict] = [
    #         {"user_key": validity.user_key, "uuid": str(validity.uuid)}
    #         for obj in res.classes.objects
    #         for validity in obj.validities
    #     ]
    #     facets = (
    #         str(validity.uuid)
    #         for obj in res.facets.objects
    #         for validity in obj.validities
    #     )

    #     # graphql may return multiple facets for a given user key, because
    #     # user keys aren't unique and because bitemporality. Instead of returning
    #     # a random UUID, we assert that they are all the same.
    #     facet_uuid = first(facets)
    #     assert all(
    #         facet == facet_uuid for facet in facets
    #     ), "facet user_key to UUID mapping is ambiguous"

    #     return (classes, facet_uuid)
