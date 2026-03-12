# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import datetime
from typing import Any
from typing import Awaitable
from typing import Callable

import pytest
from more_itertools import one
from sqlalchemy.orm import Session

from sql_export.sql_table_defs import Kvittering

from .conftest import VALIDITY


@pytest.mark.integration_test
async def test_receipt_sync(
    trigger: Callable[[], Awaitable[None]],
    create_facet: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    # Create minimal data so the export runs to completion
    await create_facet(
        {"user_key": "my_facet", "published": "Publiceret", "validity": VALIDITY}
    )

    before1 = datetime.datetime.now()
    await trigger()
    after1 = datetime.datetime.now()

    kvittering = one(actual_state_db_session.query(Kvittering).all())
    # All three timestamps should be within the test's time window
    assert before1 <= kvittering.query_tid <= after1
    assert before1 <= kvittering.start_levering_tid <= after1
    assert before1 <= kvittering.slut_levering_tid <= after1
    # Timestamps should be in chronological order
    assert kvittering.query_tid <= kvittering.start_levering_tid
    assert kvittering.start_levering_tid <= kvittering.slut_levering_tid

    # Triggering again should accumulate another receipt row
    before2 = datetime.datetime.now()
    await trigger()
    after2 = datetime.datetime.now()

    actual_state_db_session.expire_all()
    kvitteringer = (
        actual_state_db_session.query(Kvittering).order_by(Kvittering.id).all()
    )
    assert len(kvitteringer) == 2
    first, second = kvitteringer
    # The second receipt should be within the second time window
    assert before2 <= second.query_tid <= after2
    assert before2 <= second.start_levering_tid <= after2
    assert before2 <= second.slut_levering_tid <= after2
    # The second export should start after the first one finished
    assert first.slut_levering_tid <= second.query_tid
