import logging
import typing
from datetime import datetime
from pprint import pprint

from pydantic import BaseModel

from .settings import SqlExporterSettings
from .utils import append_validity_base
from .utils import build_query
from .utils import get_first_uuid_from_list
from .utils import get_list_of_uuids
from .utils import get_name
from .utils import get_org_unit_uuid
from .utils import get_person_uuid
from .utils import get_user_key
from .utils import get_uuid
from .utils import get_uuid_from_nested_dict
from .utils import get_validity_end
from .utils import get_validity_start
from .utils import GraphQLJob

logger = logging.getLogger(__name__)


class ExportBaseSemiStatic:
    query_type: str
    uuid: str

    def __init__(
        self,
        uuid: str,
        settings: SqlExporterSettings,
        historic: bool | None = None,
        skip_past: bool | None = None,
        resolve_dar: bool | None = None,
        cache: dict | None = None,
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> None:
        # super().__init__(**kwargs)
        self.uuid: str = uuid
        self.settings = settings
        self.historic: bool = historic
        if historic is None:
            self.historic = settings.historic
        self.skip_past: bool = skip_past
        if skip_past is None:
            self.skip_past = settings.skip_past
        self.resolve_dar: bool = resolve_dar
        if resolve_dar is None:
            self.resolve_dar = settings.resolve_dar
        self.cache: dict = cache or {}

    def build_from_query_result(self, obj: dict) -> None:
        pass

    def cache_obj(self) -> dict:
        return {}

    def get_query_helper(self, body: str) -> GraphQLJob:
        return build_query(
            uuid=self.uuid,
            query_type=self.query_type,
            query_body=body,
            historic=self.historic,
            skip_past=self.skip_past,
        )

    async def get_query(self) -> GraphQLJob:
        pass


class ExportBase(ExportBaseSemiStatic):
    validity_start: datetime | None
    validity_end: datetime | None

    def set_validity(self, obj: dict):
        self.validity_start = get_validity_start(obj)
        self.validity_end = get_validity_end(obj)

    def append_validity(self, body):
        return append_validity_base(body, self.validity_start, self.validity_end)


class ExportManager(ExportBase):
    query_type = "managers"

    person_uuid: str | None
    manager_level_uuid: str | None
    manager_type_uuid: str | None
    manager_responsibility_uuids: list[str] | None
    org_unit_uuid: str | None

    async def build_from_query_result(self, obj: dict) -> None:
        self.person_uuid = get_person_uuid(obj)
        self.org_unit_uuid = get_org_unit_uuid(obj)
        self.manager_type_uuid = get_uuid_from_nested_dict(obj, "manager_type")
        self.manager_level_uuid = get_uuid_from_nested_dict(obj, "manager_level")

        if obj["responsibilities"] is not None:
            self.manager_responsibility_uuids = [
                resp["uuid"] for resp in obj["responsibilities"] or None
            ]

        self.set_validity(obj)

    async def cache_obj(self) -> dict:
        body = {
            "manager_level": self.manager_level_uuid,
            "manager_responsibility": self.manager_responsibility_uuids,
            "manager_type": self.manager_type_uuid,
            "unit": self.org_unit_uuid,
            "user": self.person_uuid,
            "uuid": self.uuid,
        }

        body = self.append_validity(body)

        return {self.uuid: [body]}

    async def get_query(self) -> GraphQLJob:
        body = """
                uuid
                person {uuid}
                org_unit {uuid}
                manager_type {uuid}
                manager_level {uuid}
                responsibilities {uuid}
                validity {
                  from
                  to
                }
                """

        return self.get_query_helper(body=body)


class ExportFacet(ExportBaseSemiStatic):
    query_type = "facets"

    user_key: str | None

    async def build_from_query_result(self, obj: dict) -> None:
        self.user_key = get_user_key(obj)

    async def cache_obj(self) -> dict:
        return {self.uuid: {"user_key": self.user_key}}

    async def get_query(self) -> GraphQLJob:
        return self.get_query_helper(body="user_key")


class ExportClass(ExportBaseSemiStatic):
    query_type = "classes"

    user_key: str | None
    name: str | None
    scope: str | None
    facet: str | None

    async def build_from_query_result(self, obj: dict) -> None:
        self.user_key = get_user_key(obj)
        self.name = get_name(obj)
        self.scope = obj.get("scope")
        self.facet = get_uuid_from_nested_dict(obj, "facet")

    async def cache_obj(self) -> dict:
        body = {
            "title": self.name,
            "user_key": self.user_key,
            "scope": self.scope,
            "facet": self.facet,
        }

        return {self.uuid: body}

    async def get_query(self) -> GraphQLJob:
        body = """
               uuid
               user_key
               name
               scope
               facet {uuid}
               """
        return self.get_query_helper(body)


class ExportPerson(ExportBase):
    query_type = "employees"

    # CPR remains a string so that the exporter is versatile, and so it doesn't touch cpr numbers
    cpr_number: str | None
    user_key: str | None
    name: str | None
    given_name: str | None
    surname: str | None
    nickname: str | None
    nickname_given_name: str | None
    nickname_surname: str | None

    async def build_from_query_result(self, obj: dict) -> None:
        self.cpr_number = obj.get("cpr_number")
        self.user_key = get_user_key(obj)
        self.name = get_name(obj)
        self.given_name = obj.get("given_name")
        self.surname = obj.get("surname")
        self.nickname = obj.get("nickname")
        self.nickname_given_name = obj.get("nickname_given_name")
        self.nickname_surname = obj.get("nickname_surname")
        self.set_validity(obj)

    async def cache_obj(self) -> dict:
        body = {
            "uuid": self.uuid,
            "cpr": self.cpr_number,
            "user_key": self.user_key,
            "navn": self.name,
            "fornavn": self.given_name,
            "efternavn": self.surname,
            "kaldenavn": self.nickname,
            "kaldenavn_fornavn": self.nickname_given_name,
            "kaldenavn_efternavn": self.nickname_surname,
        }

        body = self.append_validity(body)

        return {self.uuid: [body]}

    async def get_query(self) -> GraphQLJob:
        body = """
                uuid
                cpr_number
                user_key
                name
                given_name
                surname
                nickname
                nickname_given_name
                nickname_surname
                validity {
                  from
                  to
                }
            """

        return self.get_query_helper(body)


class ExportOrgUnit(ExportBase):
    query_type = "org_units"

    user_key: str | None
    name: str | None
    unit_type_uuid: str | None
    org_unit_level_uuid: str | None
    parent_uuid: str | None
    org_unit_hierarchy_model_uuid: str | None
    manager_uuid: dict | None
    acting_manager_uuid: dict | None
    location: str | None
    primary_responsibility_class: str | None

    async def find_primary_manager(self, manager_list: list) -> str | None:
        if manager_list is None or len(manager_list) == 0:
            return None

        if self.primary_responsibility_class is None:
            return get_uuid(manager_list[0])

        for manager in manager_list:
            for responsibility in manager.get("responsibilities"):
                if str(self.primary_responsibility_class) == responsibility.get("uuid"):
                    return get_uuid(manager)

        return None

    async def build_location(self, obj: dict):
        location = get_name(obj)
        ancestors = obj.get("ancestors")
        for ancestor in ancestors:
            location = get_name(ancestor) + "\\" + location

    async def build_from_query_result(self, obj: dict) -> None:
        self.primary_responsibility_class = (
            self.settings.primary_manager_responsibility
            or self.settings.exporters_actual_state_manager_responsibility_class
        )
        self.user_key = get_user_key(obj)
        self.name = get_name(obj)
        self.parent_uuid = get_uuid_from_nested_dict(obj, "parent")
        self.unit_type_uuid = get_uuid_from_nested_dict(obj, "unit_type")
        self.org_unit_level_uuid = get_uuid_from_nested_dict(obj, "org_unit_level")
        self.org_unit_hierarchy_model_uuid = get_uuid_from_nested_dict(
            obj, "org_unit_hierarchy_model"
        )

        self.set_validity(obj)

        if not self.historic and "manager" in obj:
            self.manager_uuid = await self.find_primary_manager(obj.get("manager"))

        if not self.historic and "acting_manager" in obj:
            self.acting_manager_uuid = await self.find_primary_manager(
                obj.get("acting_manager")
            )

        if not self.historic and "ancestors" in obj:
            self.location = await self.build_location(obj)

    async def cache_obj(self) -> dict:
        body = {
            "uuid": self.uuid,
            "user_key": self.user_key,
            "name": self.name,
            "unit_type": self.unit_type_uuid,
            "level": self.org_unit_level_uuid,
            "org_unit_hierarchy": self.org_unit_hierarchy_model_uuid,
            "parent": self.parent_uuid,
        }
        if not self.historic:
            body.update({"manager_uuid": self.manager_uuid})
            body.update({"acting_manager_uuid": self.acting_manager_uuid})
            body.update({"location": self.location})

        body = self.append_validity(body)

        return {self.uuid: [body]}

    async def get_query(self) -> GraphQLJob:
        if self.historic:
            query = """
                    uuid
                    user_key
                    name
                    unit_type { uuid }
                    org_unit_level {uuid}
                    parent {uuid}
                    org_unit_hierarchy_model {uuid}
                    validity {
                        from
                        to
                    }
                    """
        else:

            query = """
                uuid
                user_key
                name
                unit_type {uuid}
                org_unit_level {uuid}
                parent {uuid}
                org_unit_hierarchy_model {uuid}
                manager: managers(inherit: false) {
                    uuid
                    responsibilities {uuid}
                }
                acting_manager: managers(inherit: true) {
                    uuid
                    responsibilities {uuid}
                }
                ancestors {
                    name
                }
                validity {
                    from
                    to
                }
                """
        return self.get_query_helper(query)


class ExportEngagement(ExportBase):
    query_type = "engagements"

    person_uuid: str | None
    org_unit_uuid: str | None
    fraction: str | None
    user_key: str | None
    engagement_type: str | None
    primary_uuid: str | None
    primary_boolean: bool | None
    job_function_uuid: str | None
    extension_1: str | None
    extension_2: str | None
    extension_3: str | None
    extension_4: str | None
    extension_5: str | None
    extension_6: str | None
    extension_7: str | None
    extension_8: str | None
    extension_9: str | None
    extension_10: str | None

    async def get_primary_boolean(self, obj) -> bool:
        primary = obj.get("primary")
        if primary is None or len(primary) == 0:
            return False
        scope = primary.get("scope")
        if scope is not None:
            return int(scope) > 0

    async def build_from_query_result(self, obj: dict) -> None:
        self.person_uuid = get_list_of_uuids(obj, "person")[0]
        self.org_unit_uuid = get_org_unit_uuid(obj)
        self.fraction = obj.get("fraction")
        self.user_key = get_user_key(obj)
        self.engagement_type = get_uuid_from_nested_dict(obj, "engagement_type")
        self.primary_uuid = get_uuid_from_nested_dict(obj, "primary")
        self.primary_boolean = await self.get_primary_boolean(obj)
        self.job_function_uuid = get_uuid_from_nested_dict(obj, "job_function")
        self.extension_1 = obj.get("extension_1")
        self.extension_2 = obj.get("extension_2")
        self.extension_3 = obj.get("extension_3")
        self.extension_4 = obj.get("extension_4")
        self.extension_5 = obj.get("extension_5")
        self.extension_6 = obj.get("extension_6")
        self.extension_7 = obj.get("extension_7")
        self.extension_8 = obj.get("extension_8")
        self.extension_9 = obj.get("extension_9")
        self.extension_10 = obj.get("extension_10")

        self.set_validity(obj)

    async def cache_obj(self) -> dict:
        extension_dict = {
            "udvidelse_1": self.extension_1,
            "udvidelse_2": self.extension_2,
            "udvidelse_3": self.extension_3,
            "udvidelse_4": self.extension_4,
            "udvidelse_5": self.extension_5,
            "udvidelse_6": self.extension_6,
            "udvidelse_7": self.extension_7,
            "udvidelse_8": self.extension_8,
            "udvidelse_9": self.extension_9,
            "udvidelse_10": self.extension_10,
        }
        body = {
            "extensions": extension_dict,
            "uuid": self.uuid,
            "user": self.person_uuid,
            "unit": self.org_unit_uuid,
            "fraction": self.fraction,
            "user_key": self.user_key,
            "engagement_type": self.engagement_type,
            "primary_type": self.primary_uuid,
            "primary_boolean": self.primary_boolean,
            "job_function": self.job_function_uuid,
        }

        body = self.append_validity(body)
        return {self.uuid: [body]}

    async def get_query(self) -> GraphQLJob:
        body = """
                uuid
                person {uuid}
                org_unit {uuid}
                fraction
                user_key
                engagement_type {uuid}
                primary {
                    user_key
                    scope
                    uuid
                }
                job_function {uuid}
                extension_1
                extension_2
                extension_3
                extension_4
                extension_5
                extension_6
                extension_7
                extension_8
                extension_9
                extension_10
                validity {
                    from
                    to
                }
            """

        return self.get_query_helper(body)


class ExportRole(ExportBase):
    query_type = "roles"

    person_uuid: str | None
    org_unit_uuid: str | None
    role_type_uuid: str | None

    async def build_from_query_result(self, obj: dict) -> None:
        self.person_uuid = get_person_uuid(obj)
        self.org_unit_uuid = get_org_unit_uuid(obj)
        self.role_type_uuid = get_uuid_from_nested_dict(obj, "role_type")
        self.set_validity(obj)

    async def cache_obj(self) -> dict:
        body = {
            "uuid": self.uuid,
            "user": self.person_uuid,
            "unit": self.org_unit_uuid,
            "role_type": self.role_type_uuid,
        }

        body = self.append_validity(body)

        return {self.uuid: [body]}

    async def get_query(self) -> GraphQLJob:
        body = """
                uuid
                person {uuid}
                org_unit {uuid}
                role_type {uuid}
                validity {
                    from
                    to
                }
            """

        return self.get_query_helper(body)


class ExportLeave(ExportBase):
    query_type = "leaves"

    person_uuid: str | None
    user_key: str | None
    leave_type: str | None
    engagement_uuid: str | None

    async def build_from_query_result(self, obj: dict) -> None:
        self.person_uuid = get_person_uuid(obj)
        self.user_key = get_user_key(obj)
        self.leave_type = get_uuid_from_nested_dict(obj, "leave_type")
        self.engagement_uuid = get_uuid_from_nested_dict(obj, "engagement")

    async def cache_obj(self) -> dict:
        body = {
            "uuid": self.uuid,
            "user": self.person_uuid,
            "user_key": self.user_key,
            "leave_type": self.leave_type,
            "engagement": self.engagement_uuid,
        }

        body = self.append_validity(body)

        return {self.uuid: [body]}

    async def get_query(self) -> GraphQLJob:
        body = """
                        uuid
                        person {uuid}
                        user_key
                        leave_type {uuid}
                        engagement {uuid}
                        validity  {
                            from
                            to
                        }
            """
        return self.get_query_helper(body)


class ExportItSystem(ExportBaseSemiStatic):
    query_type = "itsystems"

    user_key: str | None
    name: str | None

    async def build_from_query_result(self, obj: dict) -> None:
        self.user_key = get_user_key(obj)
        self.name = get_name(obj)

    async def cache_obj(self) -> dict:
        body = {"user_key": self.user_key, "name": self.name}

        self.cache.update({self.uuid: body.copy()})

    async def get_query(self) -> GraphQLJob:
        body = """
                    uuid
                    user_key
                    name
            """
        return self.get_query_helper(body)


class ExportItConnection(ExportBase):
    query_type = "itusers"

    person_uuid: str | None
    org_unit_uuid: str | None
    user_key: str | None
    itsystem_uuid: str | None
    primary_boolean: bool | None

    def get_primary_boolean(self, obj: dict) -> bool | None:
        uuid = get_uuid_from_nested_dict(obj, "primary")
        if uuid is not None and len(str(uuid)) > 0:
            return True
        return False

    async def build_from_query_result(self, obj: dict) -> None:
        self.set_validity(obj)

        self.person_uuid = get_person_uuid(obj)
        self.user_key = get_user_key(obj)
        self.org_unit_uuid = get_org_unit_uuid(obj)
        self.itsystem_uuid = get_uuid_from_nested_dict(obj, "itsystem")
        self.primary_boolean = self.get_primary_boolean(obj)

    async def cache_obj(self) -> dict:
        body = {
            "uuid": self.uuid,
            "user": self.person_uuid,
            "unit": self.org_unit_uuid,
            "user_key": self.user_key,
            "itsystem": self.itsystem_uuid,
            "primary_boolean": self.primary_boolean,
        }

        body = self.append_validity(body)

        return {self.uuid: [body]}

    async def get_query(self) -> GraphQLJob:
        body = """
                uuid
                person {uuid}
                org_unit {uuid}
                user_key
                itsystem {uuid}
                validity {
                    from
                    to
                }
                primary {uuid}
            """

        return self.get_query_helper(body)


class ExportKle(ExportBase):
    query_type = "kles"

    org_unit_uuid: str | None
    kle_number_uuid: str | None
    kle_aspects_uuid_list: list[str]
    user_key: str | None

    async def build_from_query_result(self, obj: dict) -> None:
        self.set_validity(obj)
        self.org_unit_uuid = get_org_unit_uuid(obj)
        self.user_key = get_user_key(obj)
        self.kle_number_uuid = get_uuid_from_nested_dict(obj, "kle_number")
        self.kle_aspects_uuid_list = get_list_of_uuids(obj, "kle_aspects")

    async def cache_obj(self) -> dict:
        body = {
            "uuid": self.uuid,
            "unit": self.org_unit_uuid,
            "kle_number": self.kle_number_uuid,
            "user_key": self.user_key,
        }

        body = self.append_validity(body)
        kle_obj_list = []
        for aspect in self.kle_aspects_uuid_list:
            body["kle_aspect"] = aspect

            if self.uuid in self.cache:
                self.cache[self.uuid].append(body.copy())
            else:
                self.cache.update({self.uuid: [body.copy()]})

    async def get_query(self) -> GraphQLJob:
        body = """
                uuid
                org_unit {uuid}
                kle_number {uuid}
                kle_aspects {uuid}
                user_key
                validity {
                    from
                    to
                }
            """

        return self.get_query_helper(body)


class ExportRelated(ExportBase):
    query_type = "related_units"

    org_unit_uuid_list: list[str] | None

    async def build_from_query_result(self, obj: dict) -> None:
        self.org_unit_uuid_list = get_list_of_uuids(obj, "org_units")
        self.set_validity(obj)

    async def cache_obj(self) -> dict:
        body = {"uuid": self.uuid}
        body = self.append_validity(body)

        index = 1
        for uuid in self.org_unit_uuid_list:
            body.update({f"unit{index}_uuid": uuid})
            index += 1

        return {self.uuid: [body]}

    async def get_query(self) -> GraphQLJob:
        body = """
                uuid
                org_units {uuid}
                validity {
                    from
                    to
                }
            """
        return self.get_query_helper(body)


class ExportAssociation(ExportBase):
    query_type = "associations"

    person_uuid: str | None
    org_unit_uuid: str | None
    user_key: str | None
    association_type_uuid: str | None
    it_user_uuid: str | None
    job_function_uuid: str | None
    primary_boolean: bool | None

    async def is_primary(self, obj: dict) -> bool | None:
        primary = obj.get("primary")
        if primary is None:
            return None
        return primary.get("user_key").lower() == "primary"

    async def build_from_query_result(self, obj: dict) -> None:
        self.set_validity(obj)

        self.person_uuid = get_person_uuid(obj)
        self.user_key = get_user_key(obj)
        self.org_unit_uuid = get_org_unit_uuid(obj)
        self.association_type_uuid = get_uuid_from_nested_dict(obj, "association_type")
        self.it_user_uuid = get_list_of_uuids(obj, "it_user")
        self.job_function_uuid = get_list_of_uuids(obj, "job_function")
        if self.it_user_uuid is None:
            self.job_function_uuid = None
        self.primary_boolean = await self.is_primary(obj)

    async def cache_obj(self) -> dict:
        body = {
            "uuid": self.uuid,
            "user": self.person_uuid,
            "unit": self.org_unit_uuid,
            "user_key": self.user_key,
            "association_type": self.association_type_uuid,
            "it_user": self.it_user_uuid,
            "job_function": self.job_function_uuid,
            "primary_boolean": self.primary_boolean,
        }

        body = self.append_validity(body)

        return {self.uuid: [body]}

    async def get_query(self) -> GraphQLJob:
        body = """
        uuid
        person {uuid}
        org_unit {uuid}
        user_key
        association_type {uuid}
        it_user {uuid}
        job_function {uuid}
        validity {
            from
            to
        }
        primary {
            user_key
        }
        """

        return self.get_query_helper(body)


class ExportAddress(ExportBase):
    query_type = "addresses"

    address_type_uuid: str | None
    person_uuid: str | None
    org_unit_uuid: str | None
    visibility_uuid: str | None
    name: str | None
    value: str | None
    scope: str

    scope_map = {
        "EMAIL": "E-mail",
        "WWW": "Url",
        "PHONE": "Telefon",
        "PNUMBER": "P-nummer",
        "EAN": "EAN",
        "TEXT": "Text",
        "MULTIFIELD_TEXT": "Multifield_text",
        "DAR": "DAR",
    }

    async def build_from_query_result(self, obj: dict) -> None:
        self.person_uuid = get_person_uuid(obj)
        self.name = get_name(obj)
        self.value = obj.get("value")
        self.org_unit_uuid = get_org_unit_uuid(obj)
        self.address_type_uuid = get_uuid_from_nested_dict(obj, "address_type")
        self.visibility_uuid = get_uuid_from_nested_dict(obj, "visibility")
        self.set_validity(obj)

        address_type = obj.get("address_type")
        if address_type is not None:
            self.scope = self.scope_map[address_type.get("scope")]

    async def cache_obj(self) -> dict:
        body = {
            "uuid": self.uuid,
            "user": self.person_uuid,
            "unit": self.org_unit_uuid,
            "visibility": self.visibility_uuid,
            "adresse_type": self.address_type_uuid,
            "value": self.value,
            "scope": self.scope,
        }

        if self.scope == "DAR":
            body.update({"dar_uuid": self.value})

        if self.resolve_dar:
            # We need to populate the dar cache, but do not want to
            # resolve all addresses again, and the resolved dar address we do have is
            # closest to betegnelse.
            # We are willing to overwrite an address if it is already present, as it is the
            # same address for each uuid
            body.update({"value": self.name})

        # TODO: this should be handled so it is ready to export as the dar cache
        body.update({"dar_cache_element": {self.value: {"betegnelse": self.name}}})

        body = self.append_validity(body)

        return {self.uuid: [body]}

    async def get_query(self) -> GraphQLJob:
        body = """
        person {uuid}
        org_unit {uuid}
        visibility {uuid}
        name
        value
        uuid
        address_type {
            scope
            uuid
        }
        validity {
            from
            to
        }
        """
        return self.get_query_helper(body)


mapping = {
    "facets": ExportFacet,
    "classes": ExportClass,
    "employees": ExportPerson,
    "org_units": ExportOrgUnit,
    "addresses": ExportAddress,
    "engagements": ExportEngagement,
    "managers": ExportManager,
    "associations": ExportAssociation,
    "leaves": ExportLeave,
    "roles": ExportRole,
    "itsystems": ExportItSystem,
    "itusers": ExportItConnection,
    "kles": ExportKle,
    "related_units": ExportRelated,
}


