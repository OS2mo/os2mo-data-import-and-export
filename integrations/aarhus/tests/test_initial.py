# SPDX-FileCopyrightText: 2021 Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import re
import uuid
from unittest import mock

import config
import pytest
from initial import CLASSES
from initial import InitialDataImporter
from initial import LoraClass
from ramodels.lora.klasse import Klasse


MOCK_LORA_URL = "http://example.com:8080"
MOCK_FACET_UUID = str(uuid.uuid4())


class MockConfig:
    mox_base = MOCK_LORA_URL


@pytest.mark.asyncio
async def test_loraclass_create(aioresponses):
    def lora_class_creation_response(url, **kwargs):
        # Assert that the given class UUID is the last part of the PUT URL
        assert url.path.split("/")[-1] == str(CLASSES[0].uuid)
        # Assert that the payload is a valid LoRa Klasse
        assert Klasse(**kwargs["json"])

    aioresponses.get(
        f"{MOCK_LORA_URL}/version/",
        status=200,
        payload={"lora_version": "not important"},
    )

    aioresponses.get(
        re.compile(f"^{re.escape(MOCK_LORA_URL)}/klassifikation/facet\\?bvn=\\w+$"),
        status=200,
        payload={"results": [[MOCK_FACET_UUID]]},
    )

    aioresponses.get(
        f"{MOCK_LORA_URL}/klassifikation/klasse/schema",
        status=200,
        payload={},
    )

    aioresponses.put(
        re.compile(f"^{re.escape(MOCK_LORA_URL)}/klassifikation/klasse/(.*)$"),
        status=200,
        callback=lora_class_creation_response,
    )

    with mock.patch.object(config, "get_config", return_value=MockConfig()):
        await LoraClass.create(CLASSES[0])


@pytest.mark.asyncio
async def test_initialdataimporter_import_classes():
    importer = InitialDataImporter()
    with mock.patch.object(LoraClass, "create") as mock_create:
        await importer._import_classes()
        assert mock_create.await_args_list == [mock.call(cls) for cls in CLASSES]
