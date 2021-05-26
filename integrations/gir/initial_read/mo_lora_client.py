from contextlib import asynccontextmanager
from typing import Iterable, Union

from pydantic import AnyHttpUrl
from raclients.lora import ModelClient as LoRaClient
from raclients.mo import ModelClient as MoClient
from ramodels.lora._shared import LoraBase
from ramodels.mo._shared import MOBase


class MoLoRaClient:
    def __init__(self, mo_url: AnyHttpUrl, lora_url: AnyHttpUrl):
        self.__mo_client = MoClient(base_url=mo_url)
        self.__lora_client = LoRaClient(base_url=lora_url)

    @asynccontextmanager
    async def context(self):
        try:
            async with self.__mo_client.context():
                async with self.__lora_client.context():
                    yield
        finally:
            pass

    async def load_objs(
        self,
        objs: Iterable[Union[MOBase, LoraBase]],
        disable_progressbar: bool = False,
    ):
        mos = []
        loras = []
        for obj in objs:
            if isinstance(obj, MOBase):
                mos.append(obj)
            elif isinstance(obj, LoraBase):
                loras.append(obj)
            else:
                raise TypeError(f"unexpected type: {type(obj)}")

        await self.__mo_client.load_mo_objs(
            mos, disable_progressbar=disable_progressbar
        )
        await self.__lora_client.load_lora_objs(
            loras, disable_progressbar=disable_progressbar
        )
