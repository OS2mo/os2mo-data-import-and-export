#
# Copyright (c) 2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import collections
import json
import logging
import pathlib
from functools import partial
from operator import itemgetter
from typing import Set

import sentry_sdk
from gql.client import SyncClientSession
from more_itertools import flatten
from os2sync_export import lcdb_os2mo
from os2sync_export import os2mo
from os2sync_export import os2sync
from os2sync_export.config import get_os2sync_settings
from os2sync_export.config import Settings
from os2sync_export.config import setup_gql_client
from ra_utils.tqdm_wrapper import tqdm

logger = logging.getLogger(__name__)


def log_mox_config(settings):
    """It is imperative for log-forensics to have as
    much configuration as possible logged at program start
    and end.
    """

    logger.warning("-----------------------------------------")
    logger.warning("program configuration:")
    for k, v in settings:
        logger.warning("    %s=%r", k, v)


def log_mox_counters(counter):
    logger.info("-----------------------------------------")
    logger.info("program counters:")
    for k, v in sorted(counter.items()):
        logger.info("    %s: %r", k, v)


def sync_os2sync_orgunits(settings, counter):
    logger.info("sync_os2sync_orgunits starting")

    logger.info(
        "sync_os2sync_orgunits getting " "all current organisational units from os2mo"
    )
    os2mo_uuids_present = os2mo.org_unit_uuids(
        root=settings.os2sync_top_unit_uuid,
        hierarchy_uuids=os2mo.get_org_unit_hierarchy(
            settings.os2sync_filter_hierarchy_names
        ),
    )

    logger.info(
        "sync_os2sync_orgunits getting " "units from os2mo from previous xfer date"
    )

    counter["Aktive Orgenheder fundet i OS2MO"] = len(os2mo_uuids_present)

    logger.info("sync_os2sync_orgunits upserting " "organisational units in os2sync")
    os2mo_uuids_present = tqdm(
        os2mo_uuids_present, desc="Reading org_units from OS2MO", unit="org_unit"
    )
    org_units = [
        os2mo.get_sts_orgunit(i, settings=settings) for i in os2mo_uuids_present
    ]
    return filter(None, org_units)


def read_all_user_uuids(org_uuid: str, limit: int = 1_000) -> Set[str]:
    """Return a set of all employee uuids in MO.

    :param limit: Size of pagination groups. Set to 0 to skip pagination and fetch all users in one request.
    :return: set of uuids of all employees.
    """

    start = 0
    total = 1
    all_employee_uuids = set()
    while start < total:
        employee_list = os2mo.os2mo_get(
            f"{{BASE}}/o/{org_uuid}/e/?limit={limit}&start={start}"
        ).json()

        batch = set(map(itemgetter("uuid"), employee_list["items"]))
        all_employee_uuids.update(batch)
        start = employee_list["offset"] + limit
        total = employee_list["total"]
    return all_employee_uuids


def sync_os2sync_users(gql_session: SyncClientSession, settings: Settings, counter):

    logger.info("sync_os2sync_users starting")

    logger.info(
        "sync_os2sync_users getting " "users from os2mo from previous xfer date"
    )
    org_uuid = os2mo.organization_uuid()

    logger.info("sync_os2sync_users getting list of users from os2mo")
    os2mo_uuids_present = read_all_user_uuids(org_uuid)

    counter["Medarbejdere fundet i OS2Mo"] = len(os2mo_uuids_present)

    logger.info("sync_os2sync_users upserting os2sync users")

    os2mo_uuids_present = tqdm(
        os2mo_uuids_present, desc="Reading users from OS2MO", unit="user"
    )

    all_users = flatten(
        os2mo.get_sts_user(u, gql_session=gql_session, settings=settings)
        for u in os2mo_uuids_present
    )
    return filter(None, all_users)


def main(settings: Settings):

    settings.start_logging_based_on_settings()

    if settings.os2sync_use_lc_db:
        engine = lcdb_os2mo.get_engine()
        session = lcdb_os2mo.get_session(engine)
        os2mo.get_sts_user_raw = partial(lcdb_os2mo.get_sts_user_raw, session)
        os2mo.get_sts_orgunit = partial(lcdb_os2mo.get_sts_orgunit, session)

    if settings.sentry_dsn:
        sentry_sdk.init(dsn=settings.sentry_dsn)

    hash_cache_file = pathlib.Path(settings.os2sync_hash_cache)

    counter: collections.Counter = collections.Counter()
    logger.info("mox_os2sync starting")
    log_mox_config(settings)

    if hash_cache_file and hash_cache_file.exists():
        os2sync.hash_cache.update(json.loads(hash_cache_file.read_text()))
    os2sync_client = os2sync.get_os2sync_session()
    request_uuid = os2sync.trigger_hierarchy(
        os2sync_client, os2sync_api_url=settings.os2sync_api_url
    )
    mo_org_units = sync_os2sync_orgunits(settings, counter)

    # Read from MO and update fk-org:
    for org_unit in mo_org_units:
        counter["Orgenheder som opdateres i OS2Sync"] += 1
        os2sync.upsert_orgunit(org_unit)

    # Read hierarchy from os2sync
    os2sync_hierarchy = os2sync.get_hierarchy(
        os2sync_client,
        os2sync_api_url=settings.os2sync_api_url,
        request_uuid=request_uuid,
    )
    existing_os2sync_org_units = {o["uuid"]: o for o in os2sync_hierarchy["oUs"]}
    existing_os2sync_users = {u["uuid"]: u for u in os2sync_hierarchy["users"]}

    if settings.os2sync_autowash:
        for uuid in set(existing_os2sync_org_units) - set(
            o["Uuid"] for o in mo_org_units
        ):
            counter["Orgenheder som slettes i OS2Sync"] += 1
            os2sync.delete_orgunit(uuid)

    logger.info("sync_os2sync_orgunits done")

    logger.info("Start syncing users")
    gql_client = setup_gql_client(settings)
    with gql_client as gql_session:
        mo_users = sync_os2sync_users(
            gql_session=gql_session,
            settings=settings,
            counter=counter,
        )
    # Create or update users
    for user in mo_users:
        logger.debug(f"Create or update user {user}")
        os2sync.upsert_user(user)
        counter["Medarbejdere overført til OS2SYNC"] += 1

    # Delete any user not in os2mo
    for uuid in set(existing_os2sync_users) - set(u["Uuid"] for u in mo_users):
        counter["Medarbejdere slettes i OS2Sync (pos)"] += 1
        os2sync.delete_user(uuid)

    logger.info("sync_os2sync_users done")
    if hash_cache_file:
        hash_cache_file.write_text(json.dumps(os2sync.hash_cache, indent=4))

    log_mox_counters(counter)
    log_mox_config(settings)
    logger.info("mox_os2sync done")


if __name__ == "__main__":
    settings = get_os2sync_settings()
    main(settings)
