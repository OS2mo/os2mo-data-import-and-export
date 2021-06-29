from contextlib import asynccontextmanager
from typing import Iterable, Union

from more_itertools import bucket

from os2mo_data_import.Clients.LoRa.client import Client as LoRaClient
from os2mo_data_import.Clients.LoRa.model_parts.interface import LoraObj
from os2mo_data_import.Clients.MO.client import Client as MoClient
from os2mo_data_import.Clients.MO.model_parts.interface import MoObj


class MoLoRaClient:
    def __init__(self):
        self.__mo_client = MoClient()
        self.__lora_client = LoRaClient()

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
        objs: Iterable[Union[MoObj, LoraObj]],
        disable_progressbar: bool = False,
    ):
        mos = []
        loras = []
        for obj in objs:
            if isinstance(obj, MoObj):
                mos.append(obj)
            elif isinstance(obj, LoraObj):
                loras.append(obj)
            else:
                raise TypeError(f"unexpected type: {type(obj)}")

        await self.__mo_client.load_mo_objs(
            mos, disable_progressbar=disable_progressbar
        )
        await self.__lora_client.load_lora_objs(
            loras, disable_progressbar=disable_progressbar
        )
