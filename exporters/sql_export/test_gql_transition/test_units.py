import asyncio
from ..lora_cache import LoraCache
import pydantic

async def test_units():
    lc = LoraCache(
        full_history=False,
        skip_past=True,
        resolve_dar=False
        )
    old = lc._cache_lora_units()

    async with lc.client as session:
        new = await lc._cache_lora_units_gql(session)
    assert new == old, new     

asyncio.run(test_units())

