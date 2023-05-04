import asyncio
import datetime
import logging
import os
import pickle
import time
import typing
from pathlib import Path

import aiofiles
from dateutil.parser import parse as parse_date
from gql import gql
from graphql import DocumentNode
from ra_utils.async_to_sync import async_to_sync
from ra_utils.asyncio_utils import gather_with_concurrency
from ra_utils.job_settings import JobSettings
from raclients.graph.client import PersistentGraphQLClient, GraphQLClient
from raclients.graph.util import execute_paged
from tenacity import retry
from tenacity import stop_after_delay
from tenacity import wait_exponential
from enum import Enum

RETRY_MAX_TIME = 60 * 5


class GqlLoraCacheSettings(JobSettings):
    class Config:
        pass


class QueryType(Enum):
    SIMPLE = 1
    HISTORIC = 2
    CURRENT = 3


logger = logging.getLogger(__name__)


# used to correctly insert the object into the cache
def insert_obj(obj: dict, cache: dict) -> None:
    if obj["uuid"] in cache:
        cache[obj["uuid"]].extend(obj["obj"])
    else:
        cache[obj["uuid"]] = obj["obj"]


# when getting a query using current, the object is a single dict. When getting a historic
# query it is a list of dicts. in order to uniformly process the two states we wrap the 
# current object in a list
def align_current(item: dict) -> dict:
    item["obj"] = [item["obj"]]
    return item


def convert_dict(
        query_res: dict,
        resolve_object: bool = True,
        resolve_validity: bool = True,
        replace_dict: dict = {},
) -> dict:
    def replace(d: dict, dictionary: dict):
        for replace_from, replace_to in dictionary.items():
            d[replace_to] = d.pop(replace_from)
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
            settings: GqlLoraCacheSettings | None = None,
    ):
        msg = "Start LoRa cache, resolve dar: {}, full_history: {}"
        logger.info(msg.format(resolve_dar, full_history))
        self.std_page_size = 500
        self.concurrency = 5
        self.resolve_dar = resolve_dar

        self.settings: GqlLoraCacheSettings = settings or GqlLoraCacheSettings()

        self.full_history = full_history
        self.skip_past = skip_past

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

        self.gql_queue: asyncio.Queue = asyncio.Queue()
        self.gql_client = self._setup_gql_client()

        self.settings.start_logging_based_on_settings()
        self.org_uuid = self._get_org_uuid()

    def _setup_gql_client(self) -> PersistentGraphQLClient:
        return PersistentGraphQLClient(
            url=f"{self.settings.mora_base}/graphql/v3",
            client_id=self.settings.client_id,
            client_secret=self.settings.client_secret,
            auth_realm=self.settings.auth_realm,
            auth_server=self.settings.auth_server,
            httpx_client_kwargs={"timeout": None},
            execute_timeout=None,
        )

    async def worker(self):
        while True:

            async def do(t):
                try:
                    await t
                except Exception as e:
                    # import traceback
                    import sys

                    logger.error(e)
                    # traceback.print_exc()
                    # if something goes wrong, kill it with fire!
                    # otherwise the program will just hang forever. There's probably
                    # nicer ways to do this, but this is adequately fiery
                    sys.exit(1)

            task = await self.gql_queue.get()
            await do(task)
            self.gql_queue.task_done()

    def get_historic_query(self) -> dict:
        params: typing.Dict[str, typing.Optional[str]] = {
            "to_date": str(datetime.datetime.now()),
            "from_date": str(
                (datetime.datetime.now() - datetime.timedelta(minutes=1))),
        }
        if self.full_history:
            params["to_date"] = None
            params["from_date"] = None
        if self.skip_past:
            params["from_date"] = str(
                (datetime.datetime.now() - datetime.timedelta(minutes=1))
            )
        return params

    async def construct_query(self,
                              query_type: str,
                              paged_query: bool,
                              query: str,
                              variable_values: dict | None,
                              page_size: int | None,
                              offset: int,
                              uuids: typing.List[str] | None):
        if variable_values is None:
            variable_values = {}

        query_footer = """
                                }
                            }
                        }"""
        query_header = ""

        if paged_query:
            query_header = """
                query ($limit: int, $offset: int) {
                    page: """ + query_type + """ (limit: $limit, offset: $offset){
                    """
            query_footer = """
                                        }
                                    }"""
            if page_size is None:
                page_size = self.std_page_size
            variable_values.update({'limit': page_size, 'offset': offset})

        if not paged_query:
            variable_values.update({'uuids': uuids})
        if not paged_query and not self.full_history:
            query_header = """
            query ($uuids: [UUID!]) {
                page: """ + query_type + """ (uuids: $uuids){
                    uuid
                    obj: current {"""

        if not paged_query and self.full_history:
            query_header = """
            query ($to_date: DateTime, 
                   $from_date: DateTime,
                   $uuids: [UUID!]) {
                page: """ + query_type + """ (from_date: $from_date, 
                                                 to_date: $to_date,
                                                 uuids: $uuids) {
                    uuid
                    obj: objects {
                         """
            variable_values.update(self.get_historic_query())

        return gql(query_header + query + query_footer), variable_values

    # @retry(
    #     reraise=True,
    #     wait=wait_exponential(multiplier=1, min=4, max=10),
    #     stop=stop_after_delay(RETRY_MAX_TIME),
    # )
    async def _execute_query(
            self,
            query: str,
            query_type: str,
            callback_function,
            variable_values: typing.Optional[dict] = None,
            paged_query: bool = False,
            page_size: typing.Optional[int] = None,
            offset: int = 0,
            uuids: typing.Optional[typing.List[str]] = None,
    ) -> None:
        gql_query, gql_variable_values = \
            await self.construct_query(query=query,
                                       query_type=query_type,
                                       variable_values=variable_values,
                                       page_size=page_size,
                                       offset=offset,
                                       paged_query=paged_query,
                                       uuids=uuids)

        # This does not return a dict when using get_execution_result=True
        # it returns a tuple
        result = await self.gql_client.execute(
            gql_query,
            variable_values=gql_variable_values,
            get_execution_result=True,
        )

        # therefore, this
        for obj in result.data["page"]:
            await callback_function(obj)
        if result.extensions and result.extensions.get("__page_out_of_range"):
            return

        if uuids is None:
            self.gql_queue.put_nowait(
                await self._execute_query(
                    query=query,
                    query_type=query_type,
                    variable_values=variable_values,
                    callback_function=callback_function,
                    paged_query=paged_query,
                    page_size=page_size,
                    offset=(offset + gql_variable_values['limit']),
                    uuids=uuids,
                )
            )

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
                url=f"{self.settings.mora_base}/graphql/v3",
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
        async def process_facet(query_obj):
            query_obj = convert_dict(
                query_obj, resolve_object=False, resolve_validity=False
            )
            self.facets.update(query_obj)

        query = """
                uuid
                user_key
            """

        await self._execute_query(
            query=query,
            query_type='facets',
            callback_function=process_facet,
            paged_query=True
        )

    async def _cache_lora_classes(self) -> None:
        async def process_classes(query_obj):
            dictionary = {"name": "title", "facet_uuid": "facet"}

            query_obj = convert_dict(
                query_obj,
                resolve_object=False,
                resolve_validity=False,
                replace_dict=dictionary,
            )
            self.classes.update(query_obj)

        query = """
                    uuid
                    user_key
                    name
                    scope
                    facet_uuid
            """
        await self._execute_query(
            query=query,
            query_type='classes',
            paged_query=True,
            callback_function=process_classes,
        )

    async def _cache_lora_itsystems(self) -> None:
        async def process_itsystem(query_obj):
            query_obj = convert_dict(
                query_obj, resolve_object=False, resolve_validity=False
            )
            self.itsystems.update(query_obj)

        query = """
                    uuid
                    user_key
                    name
            """

        await self._execute_query(
            query=query,
            query_type='itsystems',
            paged_query=True,
            callback_function=process_itsystem,
        )

    async def _cache_lora_users(self, uuids: typing.List[str]) -> None:
        async def process_users(query_obj):
            dictionary = {
                "cpr_no": "cpr",
                "givenname": "fornavn",
                "surname": "efternavn",
                "name": "navn",
                "nickname": "kaldenavn",
                "nickname_givenname": "kaldenavn_fornavn",
                "nickname_surname": "kaldenavn_efternavn",
            }

            if not self.full_history:
                query_obj = align_current(query_obj)

            query_obj = convert_dict(query_obj, replace_dict=dictionary)
            insert_obj(query_obj, self.users)

        query = """
                        uuid
                        cpr_no
                        user_key
                        name
                        givenname
                        surname
                        nickname
                        nickname_givenname
                        nickname_surname
                        validity {
                            from
                            to
                        }
            """

        await self._execute_query(
            query=query,
            query_type='employees',
            uuids=uuids,
            callback_function=process_users,
        )

    async def _cache_lora_units(self, uuids: typing.List[str]) -> None:
        async def process_units(query_obj):
            async def format_managers_and_location(qr: dict):
                for obj in qr["obj"]:
                    if not self.full_history:
                        if obj["manager_uuid"]:
                            obj["manager_uuid"] = obj["manager_uuid"][0]["uuid"]
                        else:
                            obj["manager_uuid"] = None

                        if obj["acting_manager_uuid"]:
                            obj["acting_manager_uuid"] = obj["acting_manager_uuid"][0][
                                "uuid"
                            ]
                        else:
                            obj["acting_manager_uuid"] = None

                        ancestors = obj.pop("ancestors")
                        location = obj["name"]
                        for ancestor in ancestors:
                            location = ancestor["name"] + "\\" + location

                        obj["location"] = location
                return qr

            if not self.full_history:
                query_obj = align_current(query_obj)

            dictionary = {
                "org_unit_level_uuid": "level",
                "org_unit_hierarchy_uuid": "org_unit_hierarchy",
                "parent_uuid": "parent",
                "unit_type_uuid": "unit_type",
            }

            for item in query_obj["obj"]:
                if item["parent_uuid"] == self.org_uuid:
                    item["parent_uuid"] = None

            query_obj = await format_managers_and_location(query_obj)

            query_obj = convert_dict(query_obj, replace_dict=dictionary)
            insert_obj(query_obj, self.units)

        if self.full_history:
            query = """
                            uuid
                            user_key
                            name
                            unit_type_uuid
                            org_unit_level_uuid
                            parent_uuid
                            org_unit_hierarchy_uuid: org_unit_hierarchy
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
                            unit_type_uuid
                            org_unit_level_uuid
                            parent_uuid
                            org_unit_hierarchy_uuid: org_unit_hierarchy
                            manager_uuid: managers(inherit: false) {
                                uuid
                            }
                            acting_manager_uuid: managers(inherit: true) {
                                uuid
                            }
                            ancestors {
                                name
                                uuid
                            }
                            validity {
                                from
                                to
                            }
                """

        await self._execute_query(
            query=query,
            query_type='org_units',
            uuids=uuids,
            callback_function=process_units,
        )

    async def _cache_lora_engagements(self, uuids: typing.List[str]) -> None:
        async def process_engagements(query_obj):
            def collect_extensions(d: dict):
                for ext_obj in d["obj"]:
                    ed = {}
                    for i in range(1, 11):
                        ed[f"udvidelse_{i}"] = ext_obj.pop(f"extension_{i}")

                    ext_obj["extensions"] = ed

                return d

            if not self.full_history:
                query_obj = align_current(query_obj)

            dictionary = {
                "employee_uuid": "user",
                "engagement_type_uuid": "engagement_type",
                "job_function_uuid": "job_function",
                "org_unit_uuid": "unit",
                "primary_uuid": "primary_type",
            }

            query_obj = collect_extensions(query_obj)
            query_obj = convert_dict(query_obj, replace_dict=dictionary)
            insert_obj(query_obj, self.engagements)

        query = """
                        uuid
                        employee_uuid
                        org_unit_uuid
                        fraction
                        user_key
                        engagement_type_uuid
                        primary_uuid
                        job_function_uuid
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

        await self._execute_query(
            query=query,
            query_type='engagements',
            uuids=uuids,
            callback_function=process_engagements,
        )

    async def _cache_lora_roles(self, uuids: typing.List[str]) -> None:
        async def process_roles(query_obj):
            if not self.full_history:
                query_obj = align_current(query_obj)

            dictionary = {
                "employee_uuid": "user",
                "org_unit_uuid": "unit",
                "role_type_uuid": "role_type",
            }

            query_obj = convert_dict(query_obj, replace_dict=dictionary)
            insert_obj(query_obj, self.roles)

        query = """
                        uuid
                        employee_uuid
                        org_unit_uuid
                        role_type_uuid
                        validity {
                            from
                            to
                        }
            """

        await self._execute_query(
            query=query,
            query_type='roles',
            uuids=uuids,
            callback_function=process_roles,
        )

    async def _cache_lora_leaves(self, uuids: typing.List[str]) -> None:
        async def process_leaves(query_obj):
            if not self.full_history:
                query_obj = align_current(query_obj)

            replace_dictionary = {
                "employee_uuid": "user",
                "leave_type_uuid": "leave_type",
                "engagement_uuid": "engagement",
            }

            query_obj = convert_dict(query_obj, replace_dict=replace_dictionary)
            insert_obj(query_obj, self.leaves)

        query = """
                        uuid
                        employee_uuid
                        user_key
                        leave_type_uuid
                        engagement_uuid
                        validity  {
                            from
                            to
                        }
            """

        await self._execute_query(
            query=query,
            query_type='leaves',
            uuids=uuids,
            callback_function=process_leaves,
        )

    async def _cache_lora_it_connections(self, uuids: typing.List[str]) -> None:
        async def process_it_connections(query_obj):
            async def set_primary_boolean(res: dict) -> dict:
                for obj in res["obj"]:
                    prim = obj.pop("primary_uuid")
                    if prim:
                        obj["primary_boolean"] = True
                    else:
                        obj["primary_boolean"] = None

                return res

            if not self.full_history:
                query_obj = align_current(query_obj)

            dictionary = {
                "employee_uuid": "user",
                "itsystem_uuid": "itsystem",
                "org_unit_uuid": "unit",
                "user_key": "username",
            }

            query_obj = await set_primary_boolean(query_obj)
            query_obj = convert_dict(query_obj, replace_dict=dictionary)
            insert_obj(query_obj, self.it_connections)

        query = """
                        uuid
                        employee_uuid
                        org_unit_uuid
                        user_key
                        itsystem_uuid
                        validity {
                            from
                            to
                        }
                        primary_uuid
            """

        await self._execute_query(
            query=query,
            query_type='itusers',
            uuids=uuids,
            callback_function=process_it_connections,
        )

    async def _cache_lora_kles(self, uuids: typing.List[str]) -> None:
        async def process_kles(query_obj):
            async def format_aspects(d: dict) -> dict:
                new_obj_list = []
                for obj in d["obj"]:
                    asp_list = []
                    asps = obj.pop("kle_aspect_uuids")
                    for a_uuid in asps:
                        aspect = obj.copy()
                        aspect["kle_aspect_uuid"] = a_uuid
                        asp_list.append(aspect)
                    new_obj_list.extend(asp_list)
                d["obj"] = new_obj_list

                return d

            if not self.full_history:
                query_obj = align_current(query_obj)

            dictionary = {
                "kle_aspect_uuid": "kle_aspect",
                "kle_number_uuid": "kle_number",
                "org_unit_uuid": "unit",
            }

            query_obj = await format_aspects(query_obj)
            query_obj = convert_dict(query_obj, replace_dict=dictionary)
            insert_obj(query_obj, self.kles)

        query = """
                        uuid
                        org_unit_uuid
                        kle_number_uuid
                        kle_aspect_uuids
                        user_key
                        validity {
                            from
                            to
                        }
            """

        await self._execute_query(
            query=query,
            query_type='kles',
            uuids=uuids,
            callback_function=process_kles,
        )

    async def _cache_lora_related(self, uuids: typing.List[str]) -> None:
        async def process_related(query_obj):
            def format_related(d: dict):
                for rel_obj in d["obj"]:
                    rel_uuids_list = rel_obj.pop("org_unit_uuids")
                    for i in range(1, (len(rel_uuids_list) + 1)):
                        rel_obj[f"unit{i}_uuid"] = rel_uuids_list[i - 1]

                return d

            if not self.full_history:
                query_obj = align_current(query_obj)

            query_obj = format_related(query_obj)

            query_obj = convert_dict(query_obj)

            insert_obj(query_obj, self.related)

        query = """
                        uuid
                        org_unit_uuids
                        validity {
                            from
                            to
                        }
            """

        await self._execute_query(
            query=query,
            query_type='related_units',
            uuids=uuids,
            callback_function=process_related,
        )

    async def _cache_lora_managers(self, uuids: typing.List[str]) -> None:
        async def process_managers(query_obj):
            if not self.full_history:
                query_obj = align_current(query_obj)

            dictionary = {
                "employee_uuid": "user",
                "manager_level_uuid": "manager_level",
                "manager_type_uuid": "manager_type",
                "responsibility_uuids": "manager_responsibility",
                "org_unit_uuid": "unit",
            }

            query_obj = convert_dict(query_obj, replace_dict=dictionary)
            insert_obj(query_obj, self.managers)

        query = """
                        uuid
                        employee_uuid
                        org_unit_uuid
                        manager_type_uuid
                        manager_level_uuid
                        responsibility_uuids
                        validity {
                            from
                            to
                        }
            """

        await self._execute_query(
            query=query,
            query_type='managers',
            uuids=uuids,
            callback_function=process_managers,
        )

    async def _cache_lora_associations(self, uuids: typing.List[str]) -> None:
        async def process_associations(query_obj):
            async def process_associations_helper(res: dict) -> dict:
                for obj in res["obj"]:
                    prim = obj.pop("primary")
                    if prim:
                        if prim["user_key"] == "primary":
                            obj["primary_boolean"] = True
                        else:
                            obj["primary_boolean"] = False
                    else:
                        obj["primary_boolean"] = None

                    if not obj["it_user_uuid"]:
                        obj["job_function_uuid"] = None

                return res

            if not self.full_history:
                query_obj = align_current(query_obj)

            replace_dict = {
                "employee_uuid": "user",
                "association_type_uuid": "association_type",
                "it_user_uuid": "it_user",
                "job_function_uuid": "job_function",
                "org_unit_uuid": "unit",
            }

            query_obj = await process_associations_helper(query_obj)
            query_obj = convert_dict(query_obj, replace_dict=replace_dict)
            insert_obj(query_obj, self.associations)

        # TODO job_function_uuid  # Should be None if it_user_uuid is None,
        #  ask Mads why?! #48316 !764 5dc0d245 . Mads is always busy
        query = """
                        uuid
                        employee_uuid
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

        await self._execute_query(
            query=query,
            query_type='associations',
            uuids=uuids,
            callback_function=process_associations,
        )

    async def _cache_lora_address(self, uuids=None):
        async def process_addresses(query_obj: dict):
            async def prep_address(d: dict) -> dict:
                for obj in d["obj"]:
                    scope = obj.pop("address_type")["scope"]

                    obj["scope"] = scope_map[scope]

                    if scope == "DAR":
                        obj["dar_uuid"] = obj["value"]
                    else:
                        obj["dar_uuid"] = None

                    if self.resolve_dar:
                        obj["value"] = obj.pop("name")
                    else:
                        obj.pop("name")

                return d

            if not self.full_history:
                query_obj = align_current(query_obj)

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

            replace_dict = {
                "employee_uuid": "user",
                "org_unit_uuid": "unit",
                "address_type_uuid": "adresse_type",
                "visibility_uuid": "visibility",
            }

            # Skip if both of the below are None, should really be an invariant in MO
            # employee_uuid
            # org_unit_uuid
            if any(
                    map(
                        lambda o: o["employee_uuid"] is None and o[
                            "org_unit_uuid"] is None,
                        query_obj["obj"],
                    )
            ):
                return

            query_obj = await prep_address(query_obj)
            query_obj = convert_dict(query_obj, replace_dict=replace_dict)
            insert_obj(query_obj, self.addresses)

        query = """
                        address_type_uuid
                        employee_uuid
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

        await self._execute_query(
            query=query,
            query_type='addresses',
            callback_function=process_addresses,
            uuids=uuids,
        )

    async def init_caching(
            self,
            skip_facets=True,
            skip_classes=True,
            skip_it_systems=True,
            skip_users=True,
            skip_units=True,
            skip_engagements=True,
            skip_roles=True,
            skip_leaves=True,
            skip_it_connections=True,
            skip_kles=True,
            skip_related=True,
            skip_managers=True,
            skip_associations=True,
            skip_addresses=True,
    ):
        async def progress(start, name):
            if start:
                logger.info(f"Starting {name} cache")
            else:
                logger.info(f"Finished caching {name}")

        async def run(query, task, name):
            objs = []
            self.gql_queue.put_nowait(progress(True, name))
            async with self._setup_gql_client() as session:
                async for obj in execute_paged(
                        gql_session=session,
                        document=query,
                        variable_values=hist,
                        per_page=5000,
                ):
                    objs.append(obj["uuid"])
                    if len(objs) == self.std_page_size:
                        self.gql_queue.put_nowait(task(uuids=objs))
                        objs = []
                self.gql_queue.put_nowait(task(uuids=objs))

            self.gql_queue.put_nowait(progress(False, name))

        logger.info("start init")
        hist = self.get_historic_query()

        if not skip_facets:
            self.gql_queue.put_nowait(progress(True, "facets"))
            self.gql_queue.put_nowait(self._cache_lora_facets())
            self.gql_queue.put_nowait(progress(False, "facets"))

        if not skip_classes:
            self.gql_queue.put_nowait(progress(True, "classes"))
            self.gql_queue.put_nowait(self._cache_lora_classes())
            self.gql_queue.put_nowait(progress(False, "classes"))

        if not skip_it_systems:
            self.gql_queue.put_nowait(progress(True, "it systems"))
            self.gql_queue.put_nowait(self._cache_lora_itsystems())
            self.gql_queue.put_nowait(progress(False, "it systems"))
        async with asyncio.TaskGroup() as tg:
            if not skip_addresses:
                addresses = gql(
                    """
                    query ($from_date: DateTime,
                           $limit: int,
                           $offset: int,
                           $to_date: DateTime) {
                        page: addresses(
                            limit: $limit
                            offset: $offset
                            from_date: $from_date
                            to_date: $to_date
                        ) {
                            uuid
                        }
                    }
                    """
                )
                tg.create_task(run(addresses, self._cache_lora_address, "addresses"))

            if not skip_units:
                units = gql(
                    """
                    query ($from_date: DateTime,
                           $limit: int,
                           $offset: int,
                           $to_date: DateTime) {
                        page: org_units(
                            limit: $limit
                            offset: $offset
                            from_date: $from_date
                            to_date: $to_date
                        ) {
                            uuid
                        }
                    }
                    """
                )
                tg.create_task(run(units, self._cache_lora_units, "org units"))

            if not skip_it_connections:
                itusers = gql(
                    """
                    query ($from_date: DateTime,
                           $limit: int,
                           $offset: int,
                           $to_date: DateTime) {
                        page: itusers(
                            limit: $limit
                            offset: $offset
                            from_date: $from_date
                            to_date: $to_date
                        ) {
                            uuid
                        }
                    }
                    """
                )
                tg.create_task(
                    run(itusers, self._cache_lora_it_connections, "it users"))

            if not skip_engagements:
                engagements = gql(
                    """
                    query ($from_date: DateTime,
                           $limit: int,
                           $offset: int,
                           $to_date: DateTime) {
                        page: engagements (
                            limit: $limit
                            offset: $offset
                            from_date: $from_date
                            to_date: $to_date
                        ) {
                            uuid
                        }
                    }
                    """
                )
                tg.create_task(
                    run(engagements, self._cache_lora_engagements, "engagements"))

            if not skip_users:
                employees = gql(
                    """
                    query ($from_date: DateTime,
                           $limit: int,
                           $offset: int,
                           $to_date: DateTime) {
                        page: employees(
                            limit: $limit
                            offset: $offset
                            from_date: $from_date
                            to_date: $to_date
                        ) {
                            uuid
                        }
                    }
                    """
                )
                tg.create_task(run(employees, self._cache_lora_users, "employees"))

            if not skip_managers:
                managers = gql(
                    """
                    query ($from_date: DateTime,
                           $limit: int,
                           $offset: int,
                           $to_date: DateTime) {
                        page: managers(
                            limit: $limit
                            offset: $offset
                            from_date: $from_date
                            to_date: $to_date
                        ) {
                            uuid
                        }
                    }
                    """
                )
                tg.create_task(run(managers, self._cache_lora_managers, "managers"))

            if not skip_associations:
                associations = gql(
                    """
                    query ($from_date: DateTime,
                           $limit: int,
                           $offset: int,
                           $to_date: DateTime) {
                        page: associations(
                            limit: $limit
                            offset: $offset
                            from_date: $from_date
                            to_date: $to_date
                        ) {
                            uuid
                        }
                    }
                    """
                )
                tg.create_task(
                    run(associations, self._cache_lora_associations, "associations"))

            if not skip_leaves:
                leaves = gql(
                    """
                    query ($from_date: DateTime,
                           $limit: int,
                           $offset: int,
                           $to_date: DateTime) {
                        page: leaves(
                            limit: $limit
                            offset: $offset
                            from_date: $from_date
                            to_date: $to_date
                        ) {
                            uuid
                        }
                    }
                    """
                )
                tg.create_task(run(leaves, self._cache_lora_leaves, "leaves"))

            if not skip_roles:
                roles = gql(
                    """
                    query ($from_date: DateTime,
                           $limit: int,
                           $offset: int,
                           $to_date: DateTime
                    ) {
                        page: roles(
                            limit: $limit
                            offset: $offset
                            from_date: $from_date
                            to_date: $to_date
                        ) {
                            uuid
                        }
                    }
                    """
                )
                tg.create_task(run(roles, self._cache_lora_roles, "roles"))

            if not skip_kles:
                kles = gql(
                    """
                    query ($from_date: DateTime,
                           $limit: int,
                           $offset: int,
                           $to_date: DateTime
                    ) {
                        page: kles(
                            limit: $limit
                            offset: $offset
                            from_date: $from_date
                            to_date: $to_date
                        ) {
                            uuid
                        }
                    }
                    """
                )
                tg.create_task(run(kles, self._cache_lora_kles, "kles"))

            if not skip_related:
                related_units = gql(
                    """
                    query ($from_date: DateTime,
                           $limit: int,
                           $offset: int,
                           $to_date: DateTime
                    ) {
                        page: related_units(
                            limit: $limit
                            offset: $offset
                            from_date: $from_date
                            to_date: $to_date
                        ) {
                            uuid
                        }
                    }
                    """
                )
                tg.create_task(
                    run(related_units, self._cache_lora_related, "related units"))

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

        t = time.time()  # noqa: F841
        msg = "Kørselstid: {:.1f}s, {} elementer, {:.0f}/s"  # noqa: F841

        await self.init_caching(
            skip_facets=False,
            skip_classes=False,
            skip_it_systems=False,
            skip_users=False,
            skip_units=False,
            skip_engagements=False,
            skip_roles=False,
            skip_leaves=False,
            skip_it_connections=False,
            skip_kles=False,
            skip_related=False,
            skip_managers=False,
            skip_associations=skip_associations,
            skip_addresses=False,
        )

        async with asyncio.TaskGroup() as tg:
            while not self.gql_queue.empty():
                tg.create_task(self.gql_queue.get_nowait())

        async def write_caches(cache, filename, name):
            logger.debug(f"writing {name}")
            if filename:
                async with aiofiles.open(filename, "wb") as fw:
                    pickle.dump(cache, fw, pickle.DEFAULT_PROTOCOL)
            logger.debug(f"done with {name}")

        async with asyncio.TaskGroup() as tg:
            tg.create_task(write_caches(self.facets, facets_file, "facets")),
            tg.create_task(
                write_caches(self.engagements, engagements_file, "engagements")),
            tg.create_task(write_caches(self.classes, classes_file, "classes")),
            tg.create_task(write_caches(self.users, users_file, "users")),
            tg.create_task(write_caches(self.units, units_file, "units")),
            tg.create_task(write_caches(self.managers, managers_file, "managers")),
            tg.create_task(write_caches(self.leaves, leaves_file, "leaves")),
            tg.create_task(write_caches(self.addresses, addresses_file, "addresses")),
            tg.create_task(write_caches(self.roles, roles_file, "roles")),
            tg.create_task(write_caches(self.itsystems, itsystems_file, "itsystems")),
            tg.create_task(write_caches(self.it_connections, it_connections_file,
                                        "it_connections")),
            tg.create_task(write_caches(self.kles, kles_file, "kles")),
            tg.create_task(write_caches(self.related, related_file, "related")),

            if not skip_associations:
                tg.create_task(
                    write_caches(self.associations, associations_file, "associations")
                )

    @async_to_sync
    async def populate_cache(self, dry_run=None, skip_associations=False):
        await self.populate_cache_async(
            dry_run=dry_run, skip_associations=skip_associations
        )
        self.gql_client.close()

    def calculate_primary_engagements(self):
        # Needed for compatibility reasons
        pass

    def calculate_derived_unit_data(self):
        # Needed for compatibility reasons
        pass
