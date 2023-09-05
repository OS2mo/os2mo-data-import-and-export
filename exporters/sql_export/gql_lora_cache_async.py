import asyncio
import datetime
import logging
import os
import pickle
import typing
from pathlib import Path
from pprint import pprint
from uuid import UUID

import flatdict
from dateutil.parser import parse as parse_date
from gql import gql
from more_itertools import first
from ra_utils.async_to_sync import async_to_sync
from ra_utils.job_settings import JobSettings
from raclients.graph.client import GraphQLClient
from tenacity import retry
from tenacity import stop_after_delay
from tenacity import wait_exponential



RETRY_MAX_TIME = 60 * 2
logger = logging.getLogger(__name__)


class GqlLoraCacheSettings(JobSettings):
    class Config:
        frozen = True

    use_new_cache: bool = False
    std_page_size: int = 500
    primary_manager_responsibility: UUID | None = None
    exporters_actual_state_manager_responsibility_class: UUID | None = None
    prometheus_pushgateway: str | None = "pushgateway"
    mox_base: str = "http://mo:5000/lora"
    persist_caches: bool = True

    def to_old_settings(self) -> dict[str, typing.Any]:
        """Convert our DatabaseSettings to a settings.json format.

        This serves to implement the adapter pattern, adapting from pydantic and its
        corresponding 12-factor configuration paradigm with environment variables, to
        the current functionality of the program, based on the settings format from
        settings.json.

        Eventually the entire settings-processing within the program should be
        rewritten with a process similar to what has been done for the SD integration,
        but it was out of scope for the change when this code was introduced.
        """

        settings = {
            "mora.base": self.mora_base,
            "mox.base": self.mox_base,
            "exporters": {
                "actual_state": {
                    "manager_responsibility_class": str(
                        self.primary_manager_responsibility
                    )
                }
            },
            "use_new_cache": self.use_new_cache,
        }

        return settings


# used to correctly insert the object into the cache
def insert_obj(obj: dict, cache: dict) -> None:
    if obj is None:
        return
    if len(obj["obj"]) == 0:
        return
    if obj["uuid"] in cache:
        cache[obj["uuid"]].extend(obj["obj"])
    else:
        cache[obj["uuid"]] = obj["obj"]


# Does various transformations on a cache to align it with the old lora cache
def convert_dict(
    query_res: dict,
    resolve_object: bool,
    resolve_validity: bool,
    replace_dict: dict,
) -> dict:
    def replace(d: dict, dictionary: dict):
        for replace_from, replace_to in dictionary.items():
            if replace_from in d.keys():
                d[replace_to] = d.pop(replace_from, None)
            else:
                index = replace_from.rfind(".")
                if index == -1:
                    d[replace_to] = None
                else:
                    replace(d, {replace_from[:index]: replace_to})
        return d

    def res_validity(d: dict):
        validity = d.pop("validity")
        if "from" in validity:
            if validity["from"]:
                d["from_date"] = str(parse_date(validity["from"]).date())
            else:
                d["from_date"] = str(datetime.datetime(1, 1, 1).date())
        if "to" in validity:
            if validity["to"]:
                d["to_date"] = str(parse_date(validity["to"]).date())
            else:
                d["to_date"] = None
        return d

    uuid = query_res.pop("uuid")
    obj_list = []
    if resolve_object:
        for obj in query_res["obj"]:
            if obj is None:
                continue
            if resolve_validity:
                obj = res_validity(obj)
            obj = replace(obj, replace_dict)
            obj_list.append(obj)

        return {"uuid": uuid, "obj": obj_list}

    if resolve_validity:
        query_res = res_validity(query_res)

    return {uuid: replace(query_res, replace_dict)}


class GQLLoraCache:
    def __init__(
        self,
        resolve_dar: bool = True,
        full_history: bool = False,
        skip_past: bool = False,
        settings=None,
    ):
        msg = "Start LoRa cache, resolve dar: {}, full_history: {}"
        logger.info(msg.format(resolve_dar, full_history))
        if isinstance(settings, dict):
            settings = None
        self.resolve_dar = resolve_dar
        self.settings: GqlLoraCacheSettings = settings or GqlLoraCacheSettings()

        self.settings.start_logging_based_on_settings()

        self.full_history = full_history
        self.skip_past = skip_past
        self.gql_client: GraphQLClient
        self.facets: dict = {}
        self.classes: dict = {}
        self.users: dict = {}
        self.units: dict = {}
        self.addresses: dict = {}
        self.engagements: dict = {}
        self.managers: dict = {}
        self.associations: dict = {}
        self.leaves: dict = {}
        self.roles: dict = {}
        self.itsystems: dict = {}
        self.it_connections: dict = {}
        self.kles: dict = {}
        self.related: dict = {}
        self.dar_cache: dict = {}

        self.org_uuid = self._get_org_uuid()

    def _setup_gql_client(self) -> GraphQLClient:
        return GraphQLClient(
            url=f"{self.settings.mora_base}/graphql/v13",
            client_id=self.settings.client_id,
            client_secret=self.settings.client_secret,
            auth_realm=self.settings.auth_realm,
            auth_server=self.settings.auth_server,
            httpx_client_kwargs={"timeout": 300},
            execute_timeout=300,
        )

    def get_limit_and_cursor_vars(self, limit: int = 0, cursor: str | None = None):
        if limit == 0:
            limit = self.settings.std_page_size
        return {"limit": limit, "cursor": cursor}



    async def construct_query(
        self,
        query_type: str,
        query: str,
        variable_values: dict | None,
    ):
        if variable_values is None:
            variable_values = {}

        query_footer = " } } } }"
        query_header = ""

        if not self.full_history:
            query_header = (
                "query ($uuids: [UUID!]) { item: " + query_type + " (uuids: $uuids){ "
                "objects { uuid obj: current { "
            )

        if self.full_history:
            query_header = (
                "query ($to_date: DateTime,"
                "$from_date: DateTime,"
                " $uuids: [UUID!]) { item: " + query_type + " (from_date: $from_date, "
                "to_date: $to_date,"
                "uuids: $uuids)"
                "{ objects { uuid obj: objects {"
            )

            variable_values.update(self.get_historic_query())
        return gql(query_header + query + query_footer), variable_values

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_delay(RETRY_MAX_TIME),
    )
    async def _execute_query(
        self,
        query: str,
        query_type: str,
        variable_values: dict,
    ):
        gql_query, gql_variable_values = await self.construct_query(
            query=query,
            query_type=query_type,
            variable_values=variable_values,
        )

        # async with self._setup_gql_client() as gql_client:
        result = await self.gql_client.execute(
            document=gql_query, variable_values=gql_variable_values
        )

        return result["page"]["objects"], result["page"]["page_info"]["next_cursor"]

    async def cache(
        self,
        query: str,
        query_type: str,
        replace_dictionary: dict,
        cache_type: dict,
        special_handling: typing.Callable | None = None,
    ):
        def align_current(item: dict) -> dict:
            item["obj"] = [item["obj"]]
            return item

        def flatten_list_of_dicts(nested):
            return [flatdict.FlatterDict(res_dict, delimiter=".") for res_dict in nested]

        def unflatten_list_of_dicts(flat):
            return [flat_d.as_dict() for flat_d in flat]

        cursor = None
        while True:
            objects, cursor = await self._execute_query(
                query=query,
                query_type=query_type,
                variable_values=self.get_limit_and_cursor_vars(cursor=cursor),
            )
            if cursor is None:
                break

            for obj in objects:
                # if not self.full_history and query_type not in ["facets", "classes", "itsystems"]:
                if not self.full_history:
                    obj = align_current(obj)

                obj['obj'] = flatten_list_of_dicts(obj['obj'])
                # if query_type == 'engagements':
                #     pprint(obj)

                if query_type == "addresses":
                    # Skip if both of the below are None, should really be an invariant in MO
                    # person_uuid
                    # org_unit_uuid
                    if any(
                        map(
                            lambda o: o is not None
                            and o["person_uuid"] is None
                            and o["org_unit_uuid"] is None,
                            obj["obj"],
                        )
                    ):
                        continue

                if special_handling is not None:
                    obj = await special_handling(obj)

                if query_type in ["facets", "classes", "itsystems"]:
                    obj = convert_dict(
                        obj,
                        replace_dict=replace_dictionary,
                        resolve_object=True,
                        resolve_validity=False,
                    )

                    for value in unflatten_list_of_dicts(obj["obj"]):
                        cache_type.update({value.pop("uuid"): value})

                else:
                    obj = convert_dict(
                        obj,
                        replace_dict=replace_dictionary,
                        resolve_object=True,
                        resolve_validity=True,
                    )
                    obj['obj'] = unflatten_list_of_dicts(obj['obj'])
                    insert_obj(obj, cache_type)

    # Used to set a value in the __init__, if this was async, init would have to be
    # as well, which would mean that the new cache could only be initiated from
    # async functions
    def _get_org_uuid(self):
        query = gql(
            """
            query {
              org {
                uuid
              }
            }
            """
        )
        with GraphQLClient(
            url=f"{self.settings.mora_base}/graphql/v13",
            client_id=self.settings.client_id,
            client_secret=self.settings.client_secret,
            auth_realm=self.settings.auth_realm,
            auth_server=self.settings.auth_server,
            sync=True,
            httpx_client_kwargs={"timeout": None},
            execute_timeout=None,
        ) as session:
            org = session.execute(query)["org"]["uuid"]
        return org

    async def _cache_lora_facets(self) -> None:
        query = """
                uuid
                user_key
            """

        await self.cache(
            query=query,
            query_type="facets",
            replace_dictionary={},
            cache_type=self.facets,
        )

    async def _cache_lora_classes(self) -> None:
        query = """
                    uuid
                    user_key
                    name
                    scope
                    facet{uuid}
            """

        dictionary = {"name": "title", "facet.uuid": "facet"}

        await self.cache(
            query=query,
            query_type="classes",
            replace_dictionary=dictionary,
            cache_type=self.classes,
        )

    async def _cache_lora_itsystems(self) -> None:
        query = """
                    uuid
                    user_key
                    name
            """

        await self.cache(
            query=query,
            query_type="itsystems",
            replace_dictionary={},
            cache_type=self.itsystems,
        )

    async def _cache_lora_users(self) -> None:
        query = """
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

        dictionary = {
            "cpr_number": "cpr",
            "given_name": "fornavn",
            "surname": "efternavn",
            "name": "navn",
            "nickname": "kaldenavn",
            "nickname_given_name": "kaldenavn_fornavn",
            "nickname_surname": "kaldenavn_efternavn",
        }

        await self.cache(
            query=query,
            query_type="employees",
            replace_dictionary=dictionary,
            cache_type=self.users,
        )

    async def _cache_lora_units(self) -> None:
        async def format_managers_and_location(qr: dict):
            def find_manager(managers) -> str | None:
                if not managers:
                    return None

                if prim_responsibility is None:
                    return managers["0.uuid"]

                for manager in managers:
                    for responsibility in manager["responsibilities"]:
                        if prim_responsibility == responsibility["uuid"]:
                            return manager["uuid"]
                return None

            prim_responsibility = None
            if self.settings.primary_manager_responsibility is not None:
                prim_responsibility = str(self.settings.primary_manager_responsibility)
            elif (
                self.settings.exporters_actual_state_manager_responsibility_class
                is not None
            ):
                prim_responsibility = str(
                    self.settings.exporters_actual_state_manager_responsibility_class
                )

            for o in qr["obj"]:
                if o is None:
                    continue
                if (
                    o["parent"] is not None
                    and "uuid" in o["parent"]
                    and o["parent"]["uuid"] == self.org_uuid
                ):
                    o["parent"]["uuid"] = None

            for man in qr["obj"]:
                if man is None:
                    continue
                if not self.full_history:
                    if man["manager_uuid"]:
                        man["manager_uuid"] = find_manager(man["manager_uuid"])
                    else:
                        man["manager_uuid"] = None

                    if man["acting_manager_uuid"]:
                        man["acting_manager_uuid"] = find_manager(man["acting_manager_uuid"])
                    else:
                        man["acting_manager_uuid"] = None

                    ancestors = man.pop("ancestors")
                    location = man["name"]
                    for i in range(len(ancestors)):
                        location = ancestors[f"{i}.name"] + "\\" + location

                    man["location"] = location
            return qr

        if self.full_history:
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
                manager_uuid: managers(inherit: false) {
                    uuid
                    responsibilities {uuid}
                }
                acting_manager_uuid: managers(inherit: true) {
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

        dictionary = {
            "org_unit_level.uuid": "level",
            "org_unit_hierarchy_model.uuid": "org_unit_hierarchy",
            "parent.uuid": "parent",
            "unit_type.uuid": "unit_type",
        }

        await self.cache(
            query=query,
            query_type="org_units",
            replace_dictionary=dictionary,
            cache_type=self.units,
            special_handling=format_managers_and_location,
        )

    async def _cache_lora_engagements(self) -> None:
        async def collect_extensions(d: dict):
            for ext_obj in d["obj"]:
                if ext_obj is None:
                    continue
                ed = {}
                for i in range(1, 11):
                    ed[f"udvidelse_{i}"] = ext_obj.pop(f"extension_{i}")

                ext_obj["extensions"] = ed

            return d

        async def set_primary_boolean(res: dict) -> dict:
            for res_obj in res["obj"]:
                if res_obj is None:
                    continue
                prim = res_obj.get("primary.scope")
                res_obj["primary_boolean"] = False
                if prim is not None:
                    res_obj["primary_boolean"] = int(prim) > 0
                    res_obj.pop('primary.scope')
                    res_obj.pop('primary.user_key')
            return res

        async def process(obj: dict) -> dict:
            obj = await collect_extensions(obj)
            return await set_primary_boolean(obj)

        query = """
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

        dictionary = {
            "person.0.uuid": "user",
            "engagement_type.uuid": "engagement_type",
            "job_function.uuid": "job_function",
            "org_unit.0.uuid": "unit",
            "primary.uuid": "primary_type",
        }

        await self.cache(
            query=query,
            query_type="engagements",
            replace_dictionary=dictionary,
            cache_type=self.engagements,
            special_handling=process,
        )

    async def _cache_lora_roles(self) -> None:
        query = """
                        uuid
                        person {uuid}
                        org_unit {uuid}
                        role_type {uuid}
                        validity {
                            from
                            to
                        }
            """

        dictionary = {
            "person.0.uuid": "user",
            "org_unit.0.uuid": "unit",
            "role_type.uuid": "role_type",
        }

        await self.cache(
            query=query,
            query_type="roles",
            replace_dictionary=dictionary,
            cache_type=self.roles,
        )

    async def _cache_lora_leaves(self) -> None:
        query = """
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

        dictionary = {
            "person.0.uuid": "user",
            "leave_type.uuid": "leave_type",
            "engagement.uuid": "engagement",
        }

        await self.cache(
            query=query,
            query_type="leaves",
            replace_dictionary=dictionary,
            cache_type=self.leaves,
        )

    async def _cache_lora_it_connections(self) -> None:
        async def set_primary_boolean(res: dict) -> dict:
            for res_obj in res["obj"]:
                if res_obj is None:
                    continue
                prim = res_obj.pop("primary")
                res_obj["primary_boolean"] = None
                if prim is not None:
                    if len(prim['uuid']) > 0:
                        res_obj["primary_boolean"] = True
            return res

        query = """
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
        dictionary = {
            "person.0.uuid": "user",
            "itsystem.uuid": "itsystem",
            "org_unit.uuid": "unit",
            "user_key": "username",
        }

        await self.cache(
            query=query,
            query_type="itusers",
            replace_dictionary=dictionary,
            cache_type=self.it_connections,
            special_handling=set_primary_boolean,
        )

    async def _cache_lora_kles(self) -> None:
        async def format_aspects(d: dict) -> dict:
            new_obj_list = []
            for kle_obj in d["obj"]:
                if kle_obj is None:
                    continue
                asp_list = []
                asps = kle_obj.pop("kle_aspects")
                for i in range(len(asps)):
                    aspect = kle_obj.copy()
                    aspect["kle_aspect_uuid"] = asps[f"{i}.uuid"]
                    asp_list.append(aspect)
                new_obj_list.extend(asp_list)
            d["obj"] = new_obj_list

            return d

        query = """
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

        dictionary = {
            "kle_aspects.uuid": "kle_aspect",
            "kle_number.uuid": "kle_number",
            "org_unit.0.uuid": "unit", #might need handling, like for each uuid in org_unit
            #format aspects
        }

        await self.cache(
            query=query,
            query_type="kles",
            replace_dictionary=dictionary,
            cache_type=self.kles,
            special_handling=format_aspects,
        )

    async def _cache_lora_related(self) -> None:
        def format_related(d: dict):
            for rel_obj in d["obj"]:
                if rel_obj is None:
                    continue
                rel_uuids_list = rel_obj.pop("org_units")
                for i in range(len(rel_uuids_list)):
                    rel_obj[f"unit{i+1}_uuid"] = rel_uuids_list[f"{i}.uuid"]

            return d

        query = """
                        uuid
                        org_units {uuid}
                        validity {
                            from
                            to
                        }
            """

        await self.cache(
            query=query,
            query_type="related_units",
            replace_dictionary={},
            cache_type=self.related,
            special_handling=format_related,
        )

    async def _cache_lora_managers(self) -> None:
        query = """
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

        dictionary = {
            "person.uuid": "user",
            "manager_level.uuid": "manager_level",
            "manager_type.uuid": "manager_type",
            "responsibilities ": "manager_responsibility",
            "org_unit.uuid": "unit",
        }

        await self.cache(
            query=query,
            query_type="managers",
            replace_dictionary=dictionary,
            cache_type=self.managers,
        )

    async def _cache_lora_associations(self) -> None:
        async def process_associations_helper(res: dict) -> dict:
            for res_obj in res["obj"]:
                if res_obj is None:
                    continue
                prim = res_obj.pop("primary")
                if prim:
                    if prim["user_key"] == "primary":
                        res_obj["primary_boolean"] = True
                    else:
                        res_obj["primary_boolean"] = False
                else:
                    res_obj["primary_boolean"] = None

                if not res_obj["it_user_uuid"]:
                    res_obj["job_function_uuid"] = None

            return res

        # TODO job_function_uuid  # Should be None if it_user_uuid is None,
        #  ask Mads why?! #48316 !764 5dc0d245 . Mads is always busy
        query = """
                        uuid
                        person_uuid
                        org_unit_uuid
                        user_key
                        association_type_uuid
                        it_user_uuid
                        job_function_uuid
                        validity {
                            from
                            to
                        }
                        primary {
                            user_key
                        }
            """

        dictionary = {
            "person_uuid": "user",
            "association_type_uuid": "association_type",
            "it_user_uuid": "it_user",
            "job_function_uuid": "job_function",
            "org_unit_uuid": "unit",
        }

        await self.cache(
            query=query,
            query_type="associations",
            replace_dictionary=dictionary,
            cache_type=self.associations,
            special_handling=process_associations_helper,
        )

    async def _cache_lora_address(self):
        async def prep_address(d: dict) -> dict:
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

            for add_obj in d["obj"]:
                if add_obj is None:
                    continue
                scope = add_obj.pop("address_type")["scope"]

                add_obj["scope"] = scope_map[scope]

                if scope == "DAR":
                    add_obj["dar_uuid"] = add_obj["value"]
                    # We need to populate the dar cache, but do not want to
                    # resolve all addresses again, and the resolved dar address we do have is
                    # closest to betegnelse.
                    # We are willing to overwrite an address if it is already present, as it is the
                    # same address for each uuid
                    self.dar_cache[add_obj["value"]] = {"betegnelse": add_obj["name"]}
                else:
                    add_obj["dar_uuid"] = None

                if self.resolve_dar:
                    add_obj["value"] = add_obj.pop("name")
                else:
                    add_obj.pop("name")

            return d

        query = """
                        address_type_uuid
                        person_uuid
                        org_unit_uuid
                        visibility_uuid
                        name
                        value
                        uuid
                        address_type {
                            scope
                        }
                        validity {
                            from
                            to
                        }
            """

        dictionary = {
            "person_uuid": "user",
            "org_unit_uuid": "unit",
            "address_type_uuid": "adresse_type",
            "visibility_uuid": "visibility",
        }

        await self.cache(
            query=query,
            query_type="addresses",
            replace_dictionary=dictionary,
            cache_type=self.addresses,
            special_handling=prep_address,
        )

    async def populate_cache_async(self, dry_run=None, skip_associations=False):
        """
        Perform the actual data import.
        :param skip_associations: If associations are not needed, they can be
        skipped for increased performance.
        :param dry_run: For testing purposes it is possible to read from cache.
        """
        if dry_run is None:
            dry_run = os.environ.get("USE_CACHED_LORACACHE", False)

        # Ensure that tmp/ exists
        Path("tmp/").mkdir(exist_ok=True)

        if self.full_history:
            if self.skip_past:
                facets_file = "tmp/facets_historic_skip_past.p"
                classes_file = "tmp/classes_historic_skip_past.p"
                users_file = "tmp/users_historic_skip_past.p"
                units_file = "tmp/units_historic_skip_past.p"
                addresses_file = "tmp/addresses_historic_skip_past.p"
                engagements_file = "tmp/engagements_historic_skip_past.p"
                managers_file = "tmp/managers_historic_skip_past.p"
                associations_file = "tmp/associations_historic_skip_past.p"
                leaves_file = "tmp/leaves_historic_skip_past.p"
                roles_file = "tmp/roles_historic_skip_past.p"
                itsystems_file = "tmp/itsystems_historic_skip_past.p"
                it_connections_file = "tmp/it_connections_historic_skip_past.p"
                kles_file = "tmp/kles_historic_skip_past.p"
                related_file = "tmp/related_historic_skip_past.p"
            else:
                facets_file = "tmp/facets_historic.p"
                classes_file = "tmp/classes_historic.p"
                users_file = "tmp/users_historic.p"
                units_file = "tmp/units_historic.p"
                addresses_file = "tmp/addresses_historic.p"
                engagements_file = "tmp/engagements_historic.p"
                managers_file = "tmp/managers_historic.p"
                associations_file = "tmp/associations_historic.p"
                leaves_file = "tmp/leaves_historic.p"
                roles_file = "tmp/roles_historic.p"
                itsystems_file = "tmp/itsystems_historic.p"
                it_connections_file = "tmp/it_connections_historic.p"
                kles_file = "tmp/kles_historic.p"
                related_file = "tmp/related_historic.p"
        else:
            facets_file = "tmp/facets.p"
            classes_file = "tmp/classes.p"
            users_file = "tmp/users.p"
            units_file = "tmp/units.p"
            addresses_file = "tmp/addresses.p"
            engagements_file = "tmp/engagements.p"
            managers_file = "tmp/managers.p"
            associations_file = "tmp/associations.p"
            leaves_file = "tmp/leaves.p"
            roles_file = "tmp/roles.p"
            itsystems_file = "tmp/itsystems.p"
            it_connections_file = "tmp/it_connections.p"
            kles_file = "tmp/kles.p"
            related_file = "tmp/related.p"

        if dry_run:
            with open(facets_file, "rb") as f:
                self.facets = pickle.load(f)
            with open(classes_file, "rb") as f:
                self.classes = pickle.load(f)
            with open(users_file, "rb") as f:
                self.users = pickle.load(f)
            with open(units_file, "rb") as f:
                self.units = pickle.load(f)
            with open(addresses_file, "rb") as f:
                self.addresses = pickle.load(f)
            with open(engagements_file, "rb") as f:
                self.engagements = pickle.load(f)
            with open(managers_file, "rb") as f:
                self.managers = pickle.load(f)

            if not skip_associations:
                with open(associations_file, "rb") as f:
                    self.associations = pickle.load(f)

            with open(leaves_file, "rb") as f:
                self.leaves = pickle.load(f)
            with open(roles_file, "rb") as f:
                self.roles = pickle.load(f)
            with open(itsystems_file, "rb") as f:
                self.itsystems = pickle.load(f)
            with open(it_connections_file, "rb") as f:
                self.it_connections = pickle.load(f)
            with open(kles_file, "rb") as f:
                self.kles = pickle.load(f)
            with open(related_file, "rb") as f:
                self.related = pickle.load(f)
            return
        async with self._setup_gql_client() as gql_client:
            self.gql_client = gql_client
            time1 = datetime.datetime.now()
            async with asyncio.TaskGroup() as tg:
                # tg.create_task(self._cache_lora_address())
                tg.create_task(self._cache_lora_units())
                tg.create_task(self._cache_lora_engagements())
                tg.create_task(self._cache_lora_facets())
                tg.create_task(self._cache_lora_classes())
                tg.create_task(self._cache_lora_users())
                tg.create_task(self._cache_lora_managers())
                # if not skip_associations:
                #     tg.create_task(self._cache_lora_associations())
                tg.create_task(self._cache_lora_leaves())
                tg.create_task(self._cache_lora_roles())
                tg.create_task(self._cache_lora_itsystems())
                tg.create_task(self._cache_lora_it_connections())
                tg.create_task(self._cache_lora_kles())
                tg.create_task(self._cache_lora_related())

        logger.info(f"populated cache in  {(datetime.datetime.now()-time1)/60} minutes")

        if self.settings.persist_caches:

            def write_caches(cache, filename, name):
                logger.debug(f"writing {name}")
                if filename:
                    with open(filename, "wb") as fw:
                        pickle.dump(cache, fw, pickle.DEFAULT_PROTOCOL)
                logger.debug(f"done with {name}")

            write_caches(self.facets, facets_file, "facets")
            write_caches(self.engagements, engagements_file, "engagements")
            write_caches(self.classes, classes_file, "classes")
            write_caches(self.users, users_file, "users")
            write_caches(self.units, units_file, "units")
            write_caches(self.managers, managers_file, "managers")
            write_caches(self.leaves, leaves_file, "leaves")
            write_caches(self.addresses, addresses_file, "addresses")
            write_caches(self.roles, roles_file, "roles")
            write_caches(self.itsystems, itsystems_file, "itsystems")
            write_caches(self.it_connections, it_connections_file, "it_connections")
            write_caches(self.kles, kles_file, "kles")
            write_caches(self.related, related_file, "related")

            if not skip_associations:
                write_caches(self.associations, associations_file, "associations")

    @async_to_sync
    async def populate_cache(self, dry_run=None, skip_associations=False):
        start_time = datetime.datetime.now()
        await self.populate_cache_async(
            dry_run=dry_run, skip_associations=skip_associations
        )
        logger.info(
            f"Populated cache in: {(datetime.datetime.now() - start_time).seconds / 60} minutes"
        )

    def calculate_primary_engagements(self):
        # Needed for compatibility reasons
        pass

    def calculate_derived_unit_data(self):
        # Needed for compatibility reasons
        pass
