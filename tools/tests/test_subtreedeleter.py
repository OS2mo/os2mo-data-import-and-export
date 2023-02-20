from typing import Optional
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest
from aiohttp import ClientSession

from tools.subtreedeleter import SubtreeDeleter

_lora_org_func_uuid = "lora-org-func-uuid"
_mo_org_uuid = "mo-org-uuid"
_mo_org_unit_uuid = "mo-org-unit-uuid"
_mo_org_unit_child_uuid = "mo-org-unit-child-uuid"
_mo_tree = [
    {
        "uuid": _mo_org_unit_uuid,
        "children": [
            {
                "uuid": _mo_org_unit_child_uuid,
            }
        ],
    },
]
_settings = {
    "mora.base": "http://mo.test-universe",
    "mox.base": "http://mox.test-universe",
}


class _TestableSubtreeDeleter(SubtreeDeleter):
    def __init__(self):
        self.mora_base = "mora"
        self.mox_base = "mox"
        self.session = ClientSession()
        self.sem = AsyncMock()
        self._deleted_org_units = []
        self._deleted_org_funcs = set()

    async def get_org_uuid(self):
        return _mo_org_uuid

    async def get_tree(self, org_uuid):
        return _mo_tree

    async def get_associated_org_func(
        self, org_unit_uuid: str, funktionsnavn: Optional[str] = None
    ):
        return [_lora_org_func_uuid]

    async def delete_from_lora(self, uuids, path):
        if path == "organisation/organisationenhed":
            self._deleted_org_units.append(uuids)
        elif path == "organisation/organisationfunktion":
            for uuid in uuids:
                self._deleted_org_funcs.add(uuid)
        else:
            raise Exception("unexpected path %r" % path)


class _Response:
    def __init__(self, doc):
        self._doc = doc

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    def raise_for_status(self):
        pass

    async def json(self):
        return self._doc


class _Session:
    def __init__(self, doc):
        self._doc = doc
        self.requests = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    def get(self, url):
        self.requests.append(("GET", url))
        return _Response(doc=self._doc)


@pytest.mark.asyncio
async def test_run_method():
    instance = _TestableSubtreeDeleter()
    await instance.run(_mo_org_unit_uuid, delete_functions=True)
    assert instance._deleted_org_units == [[_mo_org_unit_uuid, _mo_org_unit_child_uuid]]
    assert set(instance._deleted_org_funcs) == {_lora_org_func_uuid}


@pytest.mark.asyncio
@patch("tools.subtreedeleter.load_settings", return_value=_settings)
async def test_get_org_uuid(mock_load_settings):
    doc = [{"uuid": _mo_org_uuid}]
    async with _Session(doc) as session:
        instance = SubtreeDeleter(session)
        actual_org_uuid = await instance.get_org_uuid()
        assert actual_org_uuid == _mo_org_uuid
        assert session.requests == [("GET", f"{_settings['mora.base']}/service/o/")]


@pytest.mark.asyncio
@patch("tools.subtreedeleter.load_settings", return_value=_settings)
async def test_get_tree(mock_load_settings):
    async with _Session(_mo_tree) as session:
        instance = SubtreeDeleter(session)
        actual_tree = await instance.get_tree(_mo_org_uuid)
        assert actual_tree == _mo_tree
        assert session.requests == [
            ("GET", f"{_settings['mora.base']}/service/o/mo-org-uuid/ou/tree")
        ]


@pytest.mark.asyncio
@patch("tools.subtreedeleter.load_settings", return_value=_settings)
async def test_get_associated_org_func(mock_load_settings):
    doc = {"results": [_lora_org_func_uuid]}
    async with _Session(doc) as session:
        instance = SubtreeDeleter(session)
        actual_org_func_uuid = await instance.get_associated_org_func(
            _mo_org_unit_uuid,
            "funktionsnavn",
        )
        assert actual_org_func_uuid == _lora_org_func_uuid
        assert session.requests == [
            (
                "GET",
                f"{_settings['mox.base']}/organisation/organisationfunktion?tilknyttedeenheder=mo-org-unit-uuid&virkningfra=-infinity&virkningtil=infinity&funktionsnavn=funktionsnavn",
            )
        ]
