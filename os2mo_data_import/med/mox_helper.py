import asyncio
from functools import partial
from typing import Any, Sequence, Tuple
from uuid import UUID

import aiohttp
from jsonschema import validate

UUIDstr = str
aiosession = aiohttp.ClientSession


def is_uuid(string: str) -> bool:
    try:
        UUID(string)
        return True
    except ValueError:
        return False


def is_uuid_list(listy: Sequence[str]) -> bool:
    return all(list(map(is_uuid, listy)))


def ensure_session(func):
    async def _decorator(self, *args, **kwargs):
        if self.session:
            return await func(self, session, *args, **kwargs)
        else:
            async with aiohttp.ClientSession() as session:
                return await func(self, session, *args, **kwargs)

    return _decorator


class ElementNotFound(Exception):
    pass


class MultipleElementsFound(Exception):
    pass


class MoxHelper:
    def __init__(self, hostname: str, session: aiosession = None):
        self.hostname = hostname
        self.session = session

    @ensure_session
    async def check_connection(self, session: aiosession) -> bool:
        url = f"{self.hostname}/version"
        try:
            async with session.get(url) as response:
                data = await response.json()
                if "lora_version" in data:
                    return True
        except Exception:
            pass
        return False

    @ensure_session
    async def generate_methods(self, session: aiosession) -> None:
        url = f"{self.hostname}/site-map"

        async def discover_endpoints():
            async with session.get(url) as response:
                data = await response.json()
                service_urls = filter(lambda x: x.endswith("/fields"), data["site-map"])
                services = map(lambda x: x.replace("/fields", "")[1:], service_urls)
                service_tuples = map(lambda x: x.split("/"), services)
                return service_tuples

        service_tuples = await discover_endpoints()
        method_map = {
            "read_element": self._read_element,
            "read_all": self._read_uuid_list,
            "create": self._create,
            "get_or_create": self._get_or_create,
            "validate": self._validate_payload,
        }
        schema_tasks = []
        for service, obj in service_tuples:
            # Generate each method for each service / object
            for prefix, method in method_map.items():
                setattr(
                    self,
                    "_".join([prefix, service, obj]),
                    partial(method, service, obj),
                )
            # Fetch schemas for each endpoint
            task = asyncio.ensure_future(self._fetch_schema(service, obj))
            schema_tasks.append(task)
        # Fetch schemas in 'parallel'
        # TODO: Consider caching schemas
        self.schemas = dict(await asyncio.gather(*schema_tasks))

    @ensure_session
    async def _fetch_schema(
        self, session: aiosession, service: str, obj: str
    ) -> Tuple[Tuple[str, str], str]:
        url = f"{self.hostname}/{service}/{obj}/schema"
        async with session.get(url) as response:
            data = await response.json()
            return (service, obj), data

    def _validate_payload(self, service: str, obj: str, payload: Any):
        schema = self.schemas[(service, obj)]
        validate(instance=payload, schema=schema)

    @ensure_session
    async def _search(
        self, session: aiosession, service: str, obj: str, params: Any
    ) -> Sequence[str]:
        url = f"{self.hostname}/{service}/{obj}"
        async with session.get(url, params=params) as response:
            data = await response.json()
            return data["results"][0]

    @ensure_session
    async def _create(
        self, session: aiosession, service: str, obj: str, payload: Any
    ) -> UUIDstr:
        self._validate_payload(service, obj, payload)
        url = f"{self.hostname}/{service}/{obj}"
        async with session.post(url, json=payload) as response:
            return await response.json()["uuid"]

    async def _read_uuid_list(self, service: str, obj: str) -> Sequence[UUIDstr]:
        result = await self._search(service, obj, {"bvn": "%"})
        # Check that we got back valid UUIDs
        if not is_uuid_list(result):
            raise ValueError("Endpoint did not return a list of uuids")
        return result

    async def _read_element(
        self, service: str, obj: str, params: Any = None, **kwargs
    ) -> UUIDstr:
        params = params or {}
        params.update(**kwargs)
        result = await self._search(service, obj, params)
        if not is_uuid_list(result):
            raise ValueError("Endpoint did not return a list of uuids")
        if len(result) == 1:
            return result[0]
        elif len(result) > 1:
            raise MultipleElementsFound
        elif len(result) == 0:
            raise ElementNotFound

    async def _get_or_create(self, service: str, obj: str, payload) -> UUIDstr:
        bvn = payload["attributter"][obj + "egenskaber"][0]["brugervendtnoegle"]
        try:
            element = await self._read_element(service, obj, {"bvn": bvn})
            return element, False
        except ElementNotFound:
            element = await self._create(service, obj, payload)
            return element, True


async def create_mox_helper(*args, generate_methods=True, **kwargs):
    mox_helper = MoxHelper(*args, **kwargs)
    if generate_methods:
        await mox_helper.generate_methods()
    return mox_helper
