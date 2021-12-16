import asyncio

import pydantic
import pytest

from ..lora_cache import LoraCache


@pytest.mark.asyncio
async def test_units():

    lc = LoraCache(full_history=False, skip_past=False, resolve_dar=False)
    old = lc._cache_lora_units()
    async with lc.client as lc.session:
        new = await lc._cache_lora_units_gql()
    assert new.keys() == old.keys(), "Some uuids are missing"
    for uuid in new:
        assert new[uuid] == old[uuid]
