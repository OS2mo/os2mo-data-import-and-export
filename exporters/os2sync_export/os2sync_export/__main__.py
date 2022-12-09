#
# Copyright (c) 2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import collections
import datetime
import json
import logging
import pathlib
from functools import partial
from operator import itemgetter
from typing import Set

from more_itertools import flatten
from more_itertools import partition
import sentry_sdk
from os2sync_export import lcdb_os2mo
from os2sync_export import os2mo
from os2sync_export import os2sync
from os2sync_export.cleanup_mo_uuids import remove_from_os2sync
from os2sync_export.config import get_os2sync_settings
from os2sync_export.config import Settings
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


def sync_os2sync_orgunits(settings, counter, prev_date):
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
    os2mo_uuids_past = os2mo.org_unit_uuids(
        root=settings.os2sync_top_unit_uuid,
        at=prev_date,
        hierarchy_uuids=os2mo.get_org_unit_hierarchy(
            settings.os2sync_filter_hierarchy_names
        ),
    )

    counter["Aktive Orgenheder fundet i OS2MO"] = len(os2mo_uuids_present)
    counter["Orgenheder tidligere"] = len(os2mo_uuids_past)

    logger.info(
        "sync_os2sync_orgunits deleting organisational "
        "units from os2sync if deleted in os2mo"
    )
    if len(os2mo_uuids_present):
        terminated_org_units = set(os2mo_uuids_past - os2mo_uuids_present)
        for uuid in tqdm(
            terminated_org_units, desc="Deleting terminated org_units", unit="org_unit"
        ):
            counter["Orgenheder som slettes i OS2Sync"] += 1
            os2sync.delete_orgunit(uuid)

    logger.info("sync_os2sync_orgunits upserting " "organisational units in os2sync")

    for i in tqdm(os2mo_uuids_present, desc="Updating org_units", unit="org_unit"):
        sts_orgunit = os2mo.get_sts_orgunit(i, settings=settings)
        if sts_orgunit:
            counter["Orgenheder som opdateres i OS2Sync"] += 1
            os2sync.upsert_orgunit(sts_orgunit)
        elif settings.os2sync_autowash:
            counter["Orgenheder som slettes i OS2Sync"] += 1
            os2sync.delete_orgunit(i)

    logger.info("sync_os2sync_orgunits done")

    return


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


def sync_os2sync_users(settings, counter, prev_date):

    logger.info("sync_os2sync_users starting")

    logger.info(
        "sync_os2sync_users getting " "users from os2mo from previous xfer date"
    )
    org_uuid = os2mo.organization_uuid()

    logger.info("sync_os2sync_users getting list of users from os2mo")
    os2mo_uuids_present = read_all_user_uuids(org_uuid)

    counter["Medarbejdere fundet i OS2Mo"] = len(os2mo_uuids_present)

    # insert/overwrite all users from os2mo
    # maybe delete if user has no more positions
    logger.info("sync_os2sync_users upserting os2sync users")

    os2mo_uuids_present = tqdm(os2mo_uuids_present, "Updating users", unit="user")
    # medarbejdere er allerede omfattet af autowash
    # fordi de ikke får nogen 'Positions' hvis de ikke
    # har en ansættelse i en af allowed_unitids
    sts_users = flatten(
        os2mo.get_sts_user(i, settings=settings) for i in os2mo_uuids_present
    )

    to_delete, to_update = partition(lambda u: u["Positions"], sts_users)

    for i in to_delete:
        counter["Medarbejdere slettes i OS2Sync (pos)"] += 1
        os2sync.delete_user(i["uuid"])

    for i in to_update:
        os2sync.upsert_user(i)
        counter["Medarbejdere overført til OS2SYNC"] += 1

    logger.info("sync_os2sync_users done")


def main(settings: Settings):

    settings.start_logging_based_on_settings()

    if settings.os2sync_use_lc_db:
        engine = lcdb_os2mo.get_engine()
        session = lcdb_os2mo.get_session(engine)
        os2mo.get_sts_user = partial(lcdb_os2mo.get_sts_user, session)
        os2mo.get_sts_orgunit = partial(lcdb_os2mo.get_sts_orgunit, session)

    if settings.sentry_dsn:
        sentry_sdk.init(dsn=settings.sentry_dsn)

    prev_date = datetime.datetime.now() - datetime.timedelta(days=1)
    hash_cache_file = pathlib.Path(settings.os2sync_hash_cache)

    if hash_cache_file.exists():
        prev_date = datetime.datetime.fromtimestamp(hash_cache_file.stat().st_mtime)
    prev_date_str = prev_date.strftime("%Y-%m-%d")

    counter: collections.Counter = collections.Counter()
    logger.info("mox_os2sync starting")
    log_mox_config(settings)

    if hash_cache_file and hash_cache_file.exists():
        os2sync.hash_cache.update(json.loads(hash_cache_file.read_text()))

    sync_os2sync_orgunits(settings, counter, prev_date_str)
    sync_os2sync_users(settings, counter, prev_date_str)
    remove_from_os2sync(settings)

    if hash_cache_file:
        hash_cache_file.write_text(json.dumps(os2sync.hash_cache, indent=4))

    log_mox_counters(counter)
    log_mox_config(settings)
    logger.info("mox_os2sync done")


if __name__ == "__main__":
    settings = get_os2sync_settings()
    main(settings)
