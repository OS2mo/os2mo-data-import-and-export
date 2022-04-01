from uuid import uuid4

import initial
import pytest
from more_itertools import first_true
from uuids import AZID_SYSTEM

from .helpers import mock_config
from .helpers import mock_create_mox_helper


@pytest.mark.asyncio
async def test_import_it_does_nothing_if_customized():
    with mock_config(azid_it_system_uuid=uuid4()), mock_create_mox_helper() as mh:
        await initial.import_it()
        mh.assert_not_awaited()


@pytest.mark.asyncio
async def test_import_it_creates_it_system_if_not_customised():
    with mock_config(azid_it_system_uuid=AZID_SYSTEM), mock_create_mox_helper() as mh:
        await initial.import_it()
        mh.assert_awaited_once()


@pytest.mark.asyncio
async def test_import_remaining_classes():
    with mock_config(), mock_create_mox_helper():
        result = await initial.import_remaining_classes()
        non_primary_class = first_true(
            result, pred=lambda model: model.source.bvn == "non-primary"
        )
        assert non_primary_class is not None
