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

    async def populate_cache_async(self, dry_run=None, skip_associations=False):

        time1 = datetime.now()
        async with asyncio.TaskGroup() as tg:
            for key, value in mapping.items():
                tg.create_task(self.cache_init_generator(key))

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
