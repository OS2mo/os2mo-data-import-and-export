import asyncio
import logging
from asyncio.queues import Queue
from asyncio.queues import QueueEmpty
from datetime import datetime
from pprint import pprint
from typing import Any

from fastapi.encoders import jsonable_encoder
from fastramqpi.context import Context
from gql import gql
from graphql import DocumentNode
from ra_utils.async_to_sync import async_to_sync

from .classes import ExportAddress
from .classes import ExportAssociation
from .classes import ExportClass
from .classes import ExportEngagement
from .classes import ExportFacet
from .classes import ExportItConnection
from .classes import ExportItSystem
from .classes import ExportKle
from .classes import ExportLeave
from .classes import ExportManager
from .classes import ExportOrgUnit
from .classes import ExportPerson
from .classes import ExportRelated
from .classes import ExportRole
from .classes import mapping
from .data_handlers import DataHandler
from .settings import SqlExporterSettings
from .utils import get_historic_variables

logger = logging.getLogger(__name__)


class CacheJob:
    export_obj: Any
    cache: dict
    dar_cache: dict | None


class Cache:
    def __init__(
        self,
        main_org_unit_uuid,
        context: Context,
        historic: bool | None = None,
        skip_past: bool | None = None,
        resolve_dar: bool | None = None,
    ):
        self.context: Context = context
        self.user_context = context["user_context"]
        self.settings: SqlExporterSettings = self.user_context["settings"]
        self.historic = historic
        if historic is None:
            self.historic = self.settings.historic
        self.skip_past = skip_past
        if skip_past is None:
            self.skip_past = self.settings.skip_past
        self.resolve_dar = resolve_dar
        if resolve_dar is None:
            self.resolve_dar = self.settings.resolve_dar

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

        self.org_uuid = main_org_unit_uuid

        self.queue = Queue()

        self.datahandler = DataHandler(context)

    async def get_query(self, query_type: str, historic):
        if (not historic) or (query_type in ["facets", "classes", "itsystems"]):
            header = "query ($cursor: Cursor, $limit: int) { item: "

            footer = """ (limit: $limit, cursor: $cursor) {
                list_of_objects: objects {
                  uuid
                  single_obj: current {
                    uuid
                  }
                }
                page_info {
                  next_cursor
                }
              }
            }"""
        else:
            header = (
                "query ($limit: int, $cursor: Cursor, $from_date: DateTime,"
                " $to_date: DateTime) { item: "
            )

            footer = """ (
                limit: $limit
                cursor: $cursor
                from_date: $from_date
                to_date: $to_date
                ) {
                list_of_objects: objects {
                  uuid
                  single_obj: objects {
                    uuid
                  }
                }
                page_info {
                  next_cursor
                }
              }
            }"""

        return gql(header + query_type + footer)

    async def get_variables(self):
        variables = get_historic_variables(self.historic, self.skip_past)
        variables.update({"limit": self.settings.std_page_size})
        variables.update({"cursor": None})
        return variables

    async def get_cursor(self, result: dict):
        item = result.get("item")
        if item is not None:
            page_info = item.get("page_info")
            if page_info is not None:
                return page_info.get("next_cursor")
        return None

    async def get_uuid(self, result):
        item = result.get("item")
        if item is None:
            return

        objects = item.get("list_of_objects")
        if objects is None:
            return
        for element in objects:
            obj = element.get("single_obj")

            if obj is None:
                continue

            if isinstance(obj, list):
                for elem in obj:
                    uuid = elem.get("uuid")
                    if uuid is None:
                        continue
                    yield uuid
            else:
                uuid = obj.get("uuid")
                if uuid is None:
                    continue
                yield uuid

    async def get_uuids(self, query_type: str):
        query = await self.get_query(query_type, self.historic)
        variables = await self.get_variables()

        while True:
            result = await self.context["graphql_session"].execute(
                query, jsonable_encoder(variables)
            )

            next_cursor = await self.get_cursor(result)
            if next_cursor is None:
                break

            variables.update({"cursor": next_cursor})

            async for uuid in self.get_uuid(result):
                yield uuid

    async def cache_init_generator(self, query_type: str, export_type, cache: dict):
        async for uuid in self.get_uuids(query_type):
            if uuid is None:
                continue
            cache_job = CacheJob()
            cache_job.export_obj = export_type(
                uuid, self.settings, self.historic, self.skip_past, self.resolve_dar
            )
            cache_job.cache = cache
            if query_type == "addresses":
                cache_job.dar_cache = self.dar_cache
            else:
                cache_job.dar_cache = None
            await self.queue.put(cache_job)

    async def work(self):
        while not self.queue.empty():
            try:
                job: CacheJob = self.queue.get_nowait()

                list_of_populated = await self.datahandler.populate_export_obj(job.export_obj)

                for obj in list_of_populated:
                    await self.datahandler.insert_export_obj_in_cache(
                        obj, job.cache, job.dar_cache
                    )
                self.queue.task_done()

            except QueueEmpty:
                break

    async def get_facets_uuids(self):
        query_type = "facets"
        export_type = ExportFacet
        await self.cache_init_generator(query_type, export_type, self.facets)

    async def get_classes_uuids(self):
        query_type = "classes"
        export_type = ExportClass
        await self.cache_init_generator(query_type, export_type, self.classes)

    async def get_users_uuids(self):
        query_type = "employees"
        export_type = ExportPerson
        await self.cache_init_generator(query_type, export_type, self.users)

    async def get_units_uuids(self):
        query_type = "org_units"
        export_type = ExportOrgUnit
        await self.cache_init_generator(query_type, export_type, self.units)

    async def get_addresses_uuids(self):
        query_type = "addresses"
        export_type = ExportAddress
        await self.cache_init_generator(query_type, export_type, self.addresses)

    async def get_engagements_uuids(self):
        query_type = "engagements"
        export_type = ExportEngagement
        await self.cache_init_generator(query_type, export_type, self.engagements)

    async def get_managers_uuids(self):
        query_type = "managers"
        export_type = ExportManager
        await self.cache_init_generator(query_type, export_type, self.managers)

    async def get_associations_uuids(self):
        query_type = "associations"
        export_type = ExportAssociation
        await self.cache_init_generator(query_type, export_type, self.associations)

    async def get_leaves_uuids(self):
        query_type = "leaves"
        export_type = ExportLeave
        await self.cache_init_generator(query_type, export_type, self.leaves)

    async def get_roles_uuids(self):
        query_type = "roles"
        export_type = ExportRole
        await self.cache_init_generator(query_type, export_type, self.roles)

    async def get_itsystems_uuids(self):
        query_type = "itsystems"
        export_type = ExportItSystem
        await self.cache_init_generator(query_type, export_type, self.itsystems)

    async def get_it_connections_uuids(self):
        query_type = "itusers"
        export_type = ExportItConnection
        await self.cache_init_generator(query_type, export_type, self.it_connections)

    async def get_kles_uuids(self):
        query_type = "kles"
        export_type = ExportKle
        await self.cache_init_generator(query_type, export_type, self.kles)

    async def get_related_uuids(self):
        query_type = "related_units"
        export_type = ExportRelated
        await self.cache_init_generator(query_type, export_type, self.related)

    async def populate_cache_async(self, dry_run=None, skip_associations=False):

        time1 = datetime.now()
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.get_facets_uuids())
            tg.create_task(self.get_classes_uuids())
            tg.create_task(self.get_users_uuids())
            tg.create_task(self.get_units_uuids())
            tg.create_task(self.get_addresses_uuids())
            tg.create_task(self.get_engagements_uuids())
            tg.create_task(self.get_managers_uuids())
            if not skip_associations:
                tg.create_task(self.get_associations_uuids())
            tg.create_task(self.get_leaves_uuids())
            tg.create_task(self.get_roles_uuids())
            tg.create_task(self.get_itsystems_uuids())
            tg.create_task(self.get_it_connections_uuids())
            tg.create_task(self.get_kles_uuids())
            tg.create_task(self.get_related_uuids())

        logger.info(f"got uuids in  {(datetime.now()-time1)} minutes")
        print(f"got uuids in  {(datetime.now()-time1)} minutes")

        print("finished getting uuids, starting caching")
        time1 = datetime.now()
        async with asyncio.TaskGroup() as workers:
            for i in range(25):
                workers.create_task(self.work())

        await self.queue.join()
        print(f"populated full cache in  {(datetime.now()-time1)} minutes")

    @async_to_sync
    async def populate_cache(self, dry_run=None, skip_associations=False):
        start_time = datetime.now()
        await self.populate_cache_async(
            dry_run=dry_run, skip_associations=skip_associations
        )
        logger.info(
            f"Populated cache in: {(datetime.now() - start_time).seconds / 60} minutes"
        )
