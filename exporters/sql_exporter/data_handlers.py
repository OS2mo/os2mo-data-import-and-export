import logging
from pprint import pprint

from fastapi.encoders import jsonable_encoder
from fastramqpi.context import Context
from gql import gql
from copy import deepcopy, copy

from .classes import ExportAddress
from .classes import GraphQLJob
from .settings import SqlExporterSettings
from tenacity import retry
from tenacity import stop_after_delay
from tenacity import wait_exponential

RETRY_MAX_TIME = 60 * 2
logger = logging.getLogger(__name__)


class DataHandler:
    def __init__(self, context: Context):
        self.context: Context = context
        self.user_context = context["user_context"]
        self.settings: SqlExporterSettings = self.user_context["settings"]

    async def insert_export_obj_in_cache(
        self, export_obj, cache: dict, dar_cache: dict | None = None
    ) -> None:
        job = await export_obj.dict()

        for key, value in job.items():
            if isinstance(value, list):
                #
                # if isinstance(export_obj, ExportAddress):
                #     dar = value.get("dar_cache_element")
                #     if dar is not None:
                #         dar_cache.update(dar)
                #         value.pop("dar_cache_element")

                if key in cache:
                    cache[key].append(value)
                else:
                    cache[key] = value
            else:
                cache[key] = value

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_delay(RETRY_MAX_TIME),
    )
    async def populate_export_obj(self, export_obj):
        job: GraphQLJob = await export_obj.get_query()

        query = gql(job.query)
        result = await self.context.get("graphql_session").execute(
            query, jsonable_encoder(job.variables)
        )

        export = []
        item = result.get("item")
        for data in item["list_of_objects"]:
            obj = data.get('single_obj')
            if not isinstance(obj, list):
                obj = [obj]
            for entry in obj:
                populated = copy(export_obj)
                await populated.build_from_query_result(entry)
                export.append(populated)
        return export

