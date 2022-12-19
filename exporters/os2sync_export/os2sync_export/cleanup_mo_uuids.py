# SPDX-FileCopyrightText: 2022 Magenta ApS
# SPDX-License-Identifier: MPL-2.0
import logging
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from uuid import UUID

from gql import gql
from gql.client import SyncClientSession
from more_itertools import one
from more_itertools import partition
from os2sync_export.config import Settings
from os2sync_export.os2sync import delete_orgunit
from os2sync_export.os2sync import delete_user

logger = logging.getLogger(__name__)


def get_it_user_uuids(gql_session: SyncClientSession, settings: Settings) -> List:
    """Read all MO uuids that have it-accounts."""

    query = gql(
        """query MyQuery {
              itusers {
                objects {
                  employee_uuid
                  org_unit_uuid
                  itsystem {
                    name
                  }
                }
              }
            }
        """
    )

    r = gql_session.execute(query)
    # Filter by it-systems
    filtered_uuids = filter(
        lambda it: one(it["objects"])["itsystem"]["name"]
        in settings.os2sync_uuid_from_it_systems,
        r["itusers"],
    )

    return list(filtered_uuids)


def extract_uuids(gql_response: List) -> Tuple[Set[UUID], Set[UUID]]:
    """Split all it-user uuids into sets of org_unit uuids and employee uuids"""

    # Split into org_units and employees
    org_units, employees = partition(
        lambda x: x["objects"][0]["employee_uuid"], gql_response
    )
    # extract uuids
    org_unit_uuids = set(UUID(one(e["objects"])["org_unit_uuid"]) for e in org_units)
    employee_uuids = set(UUID(one(e["objects"])["employee_uuid"]) for e in employees)

    return org_unit_uuids, employee_uuids


def remove_from_os2sync(
    gql_session: SyncClientSession, settings: Settings, dry_run: bool = False
) -> Optional[Tuple[Set[UUID], Set[UUID]]]:

    if not settings.os2sync_uuid_from_it_systems:
        # No need to check it-accounts.
        return None
    logger.info("Checking for uuids from it-systems")
    # Read it-users
    uuids = get_it_user_uuids(gql_session=gql_session, settings=settings)

    # Split into units and employees
    org_unit_uuids, employee_uuids = extract_uuids(uuids)
    logger.info(
        f"Possible duplicates: org_units={len(org_unit_uuids)}, employees={len(employee_uuids)}"
    )
    if dry_run:
        return org_unit_uuids, employee_uuids

    # Delete
    for uuid in org_unit_uuids:
        delete_orgunit(str(uuid))
    for uuid in employee_uuids:
        delete_user(str(uuid))

    return org_unit_uuids, employee_uuids
