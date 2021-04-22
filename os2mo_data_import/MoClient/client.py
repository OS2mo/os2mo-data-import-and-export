import logging
from asyncio import Semaphore, as_completed, gather, run, sleep
from contextlib import asynccontextmanager
from itertools import groupby
from typing import Callable, Coroutine, Iterable, List, Optional, Tuple, Type

from aiohttp import ClientSession
from more_itertools import chunked
from tqdm import tqdm

from os2mo_data_import.MoClient.model import (
    Address,
    Employee,
    Engagement,
    EngagementAssociation,
    Facet,
    Klasse,
    Manager,
    Organisation,
    OrgUnit,
)
from os2mo_data_import.MoClient.model_parts.interface import MoObj

logger = logging.getLogger(__name__)


class Client:
    __mo_path_map = {
        OrgUnit: "/service/ou/create",
        Employee: "/service/e/create",
        Engagement: "/service/details/create",
        EngagementAssociation: "/service/details/create",
        Manager: "/service/details/create",
        Address: "/service/details/create",
    }
    __mox_path_map = {
        Organisation: "/organisation/organisation",
        Facet: "/klassifikation/facet",
        Klasse: "/klassifikation/klasse",
    }

    def __init__(
        self,
        session_factory: Callable[[], ClientSession] = ClientSession,
        sem_size: int = 1,
        chunk_size: int = 100,
        base_url="http://localhost:5000",
        base_mox_url="http://localhost:8080",
    ):
        # connection logic
        self.__sem = Semaphore(sem_size)
        self.__chunk_size = chunk_size
        self.__session_factory = session_factory
        self.__base_url = base_url
        self.__base_mox_url = base_mox_url
        self.__session: Optional[ClientSession] = None

    @asynccontextmanager
    async def context(self):
        try:
            async with self.__session_factory() as session:
                self.__session = session
                await self.__check_if_server_online()

                yield
        finally:
            self.__session = None

    async def load_mo_objs(
        self, objs: Iterable[MoObj], disable_progressbar: bool = False
    ):
        """
        lazy init client session to ensure created within async context
        :param objs:
        :param disable_progressbar:
        :return:
        """
        await self.__submit_payloads(objs, disable_progressbar=disable_progressbar)

    async def __verify_session(self):
        if self.__session is None:
            raise Exception("Need to initialize client session!")

    async def __check_if_server_online(self, attempts=100, delay=1):
        """
        check if server is online
        :param attempts: Number of repeats
        :param delay: Number of sleeps in-between
        :return:
        """
        await self.__verify_session()
        async with self.__session_factory() as session:
            async def check_endpoint(url, response):
                for _ in range(attempts):
                    try:
                        resp = await session.get(url)
                        resp.raise_for_status()
                        if response in await resp.json():
                            return
                        raise Exception("Invalid response")
                    except Exception:
                        await sleep(delay)
                raise Exception("Unable to connect")

            tasks = [
                (self.__base_url + "/version/", "mo_version"),
                (self.__base_mox_url + "/version", "lora_version"),
            ]
            tasks = starmap(check_endpoint, *unzip(tasks))
            await asyncio.gather(*tasks)

    async def __post_to_mo(self, current_type: Type[MoObj], data: Iterable[MoObj]):
        await self.__verify_session()
        jsons = [x.dict(by_alias=True) for x in data]
        for json in jsons:
            async with self.__sem:
                async with self.__session.post(
                    self.__base_url + self.__mo_path_map[current_type], json=json
                ) as response:
                    response.raise_for_status()

    async def __post_single_to_mox(self, current_type: Type[MoObj], obj: MoObj):
        """

        :param current_type: Redundant, only pass it because we already have it
        :param data:
        :return:
        """
        await self.__verify_session()

        async with self.__sem:
            uuid = obj.get_uuid()
            # TODO, PENDING: https://github.com/samuelcolvin/pydantic/pull/2231
            # for now, uuid is included, and has to be excluded when converted to json
            jsonified = obj.dict(by_alias=True, exclude={"uuid"}, exclude_none=True)
            generic_url = self.__base_mox_url + self.__mox_path_map[current_type]
            if uuid is None:  # post
                async with self.__session.post(
                    generic_url,
                    json=jsonified,
                ) as response:
                    response.raise_for_status()
            else:  # put
                async with self.__session.put(
                    generic_url + f"/{uuid}",
                    json=jsonified,
                ) as response:
                    response.raise_for_status()

    async def __post_to_mox(self, current_type: Type[MoObj], data: Iterable[MoObj]):
        """
        wrapper allows passing list of mox objs, for individual posting
        :param current_type:
        :param data:
        :return:
        """
        await gather(
            *map(
                lambda obj: self.__post_single_to_mox(
                    current_type=current_type, obj=obj
                ),
                data,
            )
        )

    async def __submit(self, data: Iterable[MoObj]):
        """
        maps the object appropriately to either MO or LoRa

        :param data: An iterable of objects of the *same* type!
        :return:
        """

        data = list(data)
        current_type = type(data[0])

        assert all([isinstance(obj, current_type) for obj in data])

        if current_type in self.__mo_path_map:
            await self.__post_to_mo(current_type, data)
        elif current_type in self.__mox_path_map:
            await self.__post_to_mox(current_type, data)
        raise TypeError(f"unknown type: {current_type}")

    async def __submit_payloads(
        self, objs: Iterable[MoObj], disable_progressbar=False
    ):
        objs = list(objs)
        groups = groupby(objs, lambda x: type(x).__name__)
        chunked_groups: List[Tuple[str, Iterable[List[MoObj]]]] = [
            (type_name, list(chunked(objs, self.__chunk_size)))
            for type_name, objs in groups
        ]
        chunked_tasks: List[Tuple[str, List[Coroutine]]] = [
            (type_name, list(map(self.__submit, chunks)))
            for type_name, chunks in chunked_groups
        ]
        if not chunked_tasks or all([not tasks for _, tasks in chunked_tasks]):
            return
        for key, tasks in chunked_tasks:
            for f in tqdm(
                as_completed(tasks),
                total=len(tasks),
                unit=f"chunk",
                desc=key,
                disable=disable_progressbar,
            ):
                await f


if __name__ == "__main__":
    c = Client()
    run(
        c.load_mo_objs(
            [
                Organisation.from_simplified_fields(
                    uuid="456362c4-0ee4-4e5e-a72c-751239745e64",
                    name="test_org_name",
                    user_key="test_org_user_key",
                )
            ]
        )
    )
