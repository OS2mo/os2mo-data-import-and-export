# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
"""Event-driven export to the historic (full-history) export DB.

The event-driven app runs two exporters when ``HISTORIC_STATE`` is configured:
the actual-state exporter (``current`` validity) and the historic exporter
(all validities). These tests assert both targets, including the defining
difference: a terminated (now past) period is dropped from actual-state but
retained in historic.

Note: ``skip_past`` is not configurable in the event-driven app (it is hardcoded
to ``False`` and only settable via the CLI), so the historic exporter always
retains past periods. The ``skip_past=True`` combination is therefore not
reachable here and is out of scope.
"""

from typing import Any
from typing import Awaitable
from typing import Callable

import pytest
from sqlalchemy.orm import Session

from sql_export.sql_table_defs import Bruger
from sql_export.sql_table_defs import ItForbindelse

from ..conftest import VALIDITY
from .conftest import assert_absent
from .conftest import assert_row


def bruger(uuid: str) -> dict[str, Any]:
    return {
        "uuid": uuid,
        "bvn": "user_key",
        "fornavn": "given_name",
        "efternavn": "surname",
        "kaldenavn_fornavn": "",
        "kaldenavn_efternavn": "",
        "cpr": "0101700000",
        "startdato": "1970-01-01",
        "slutdato": "9999-12-31",
    }


@pytest.mark.integration_test
async def test_actual_and_historic_export(
    server: None,
    purge_historic_export_db: None,
    create_person: Callable[[dict[str, Any]], Awaitable[str]],
    delete: Callable[[str, str], Awaitable[None]],
    actual_state_db_session: Session,
    historic_state_db_session: Session,
) -> None:
    """A created entity is written to both export targets, and hard-delete
    removes it from both."""
    uuid = await create_person(
        {
            "cpr_number": "0101700000",
            "given_name": "given_name",
            "surname": "surname",
            "user_key": "user_key",
        }
    )
    await assert_row(actual_state_db_session, Bruger, bruger(uuid))
    await assert_row(historic_state_db_session, Bruger, bruger(uuid))

    await delete("employee", uuid)
    await assert_absent(actual_state_db_session, Bruger)
    await assert_absent(historic_state_db_session, Bruger)


@pytest.mark.integration_test
async def test_historic_retains_terminated_period(
    server: None,
    purge_historic_export_db: None,
    create_person: Callable[[dict[str, Any]], Awaitable[str]],
    create_it_system: Callable[[dict[str, Any]], Awaitable[str]],
    create_it_connection: Callable[[dict[str, Any]], Awaitable[str]],
    terminate: Callable[[str, str], Awaitable[None]],
    actual_state_db_session: Session,
    historic_state_db_session: Session,
) -> None:
    """After termination into the past, the entity is no longer ``current`` (so
    it is dropped from actual-state) but its closed period is retained in the
    historic export."""
    person_uuid = await create_person(
        {
            "cpr_number": "0404700000",
            "given_name": "IT",
            "surname": "User",
            "user_key": "it_user",
        }
    )
    it_system_uuid = await create_it_system(
        {"name": "My System", "user_key": "my_system", "validity": VALIDITY}
    )
    uuid = await create_it_connection(
        {
            "user_key": "it_username",
            "person": person_uuid,
            "itsystem": it_system_uuid,
            "validity": VALIDITY,
        }
    )

    def expected(slutdato: str) -> dict[str, Any]:
        return {
            "uuid": uuid,
            "it_system_uuid": it_system_uuid,
            "bruger_uuid": person_uuid,
            "enhed_uuid": None,
            "brugernavn": "it_username",
            "eksternt_id": None,
            "primær_boolean": None,
            "startdato": "2020-01-01",
            "slutdato": slutdato,
        }

    await assert_row(actual_state_db_session, ItForbindelse, expected("9999-12-31"))
    await assert_row(historic_state_db_session, ItForbindelse, expected("9999-12-31"))

    # Terminate into the past (TERMINATE_TO = 2021-01-01).
    await terminate("ituser", uuid)

    # Dropped from actual-state (no longer current)...
    await assert_absent(actual_state_db_session, ItForbindelse)
    # ...but the now-closed period is retained in historic.
    await assert_row(historic_state_db_session, ItForbindelse, expected("2021-01-01"))
