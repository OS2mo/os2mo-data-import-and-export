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

from more_itertools import partition

from integrations.os2sync import config
from integrations.os2sync import lcdb_os2mo
from integrations.os2sync import os2mo
from integrations.os2sync import os2sync

logger = None  # set in main()


def log_mox_config(settings):
    """It is imperative for log-forensics to have as
    much configuration as possible logged at program start
    and end.
    """
    secrets = ["OS2MO_SAML_TOKEN"]
    logger.warning("-----------------------------------------")
    logger.warning("program configuration:")
    for k, v in sorted(settings.items()):
        if k in secrets:
            logger.warning("    %s=********", k)
        else:
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
    os2mo_uuids_present = set(os2mo.org_unit_uuids())

    logger.info(
        "sync_os2sync_orgunits getting " "units from os2mo from previous xfer date"
    )
    os2mo_uuids_past = set(os2mo.org_unit_uuids(at=prev_date))

    counter["Aktive Orgenheder fundet i OS2MO"] = len(os2mo_uuids_present)
    counter["Orgenheder tidligere"] = len(os2mo_uuids_past)

    is_deleted_from_MO = lambda org_uuid: org_uuid in set(
        os2mo_uuids_past - os2mo_uuids_present
    )
    os2mo_uuids, uuids_deleted = partition(is_deleted_from_MO, os2mo_uuids_present)
    os2mo_uuids, uuids_deleted = set(os2mo_uuids), set(uuids_deleted)
    org_units = list(map(os2mo.get_sts_orgunit, os2mo_uuids))
    org_units = list(filter(None.__ne__, org_units))
    allowed_unitids = set(map(itemgetter("Uuid"), org_units))
    filtered = os2mo_uuids - allowed_unitids

    counter["Orgenheder som slettes i OS2Sync"] += len(uuids_deleted)

    list(map(os2sync.delete_orgunit, uuids_deleted))

    logger.info("Updating organisational units in os2sync")

    counter["Orgenheder som opdateres i OS2Sync"] += len(org_units)
    list(map(os2sync.upsert_orgunit, org_units))
    if settings["OS2SYNC_AUTOWASH"]:
        counter["Orgenheder som slettes i OS2Sync"] += filtered
        list(map(os2sync.delete_orgunit, filtered))

    logger.info("sync_os2sync_orgunits done")

    return set(allowed_unitids)


def sync_os2sync_users(settings, allowed_unitids, counter, prev_date):

    logger.info("sync_os2sync_users starting")

    logger.info(
        "sync_os2sync_users getting " "users from os2mo from previous xfer date"
    )
    os2mo_uuids_past = set(os2mo.user_uuids(at=prev_date))

    logger.info("sync_os2sync_users getting list of users from os2mo")
    os2mo_uuids_present = set(os2mo.user_uuids())

    counter["Medarbejdere fundet i OS2Mo"] = len(os2mo_uuids_present)
    counter["Medarbejdere tidligere"] = len(os2mo_uuids_past)

    logger.info("sync_os2sync_users deleting " "os2mo-deleted users in os2sync")

    if len(os2mo_uuids_present):
        for uuid in set(os2mo_uuids_past - os2mo_uuids_present):
            counter["Medarbejdere slettes i OS2Sync (del)"] += 1
            os2sync.delete_user(uuid)

    # insert/overwrite all users from os2mo
    # maybe delete if user has no more positions
    logger.info("sync_os2sync_users upserting os2sync users")

    for i in os2mo_uuids_present:
        # medarbejdere er allerede omfattet af autowash
        # fordi de ikke får nogen 'Positions' hvis de ikke
        # har en ansættelse i en af allowed_unitids
        sts_user = os2mo.get_sts_user(i, allowed_unitids)

        if not sts_user["Positions"]:
            counter["Medarbejdere slettes i OS2Sync (pos)"] += 1
            os2sync.delete_user(i)
            continue

        os2sync.upsert_user(sts_user)
        counter["Medarbejdere overført til OS2SYNC"] += 1

    logger.info("sync_os2sync_users done")


def main(settings):
    # set warning-level for all loggers
    global logger
    [
        logging.getLogger(name).setLevel(logging.WARNING)
        for name in logging.root.manager.loggerDict
        if name != config.loggername
    ]

    logging.basicConfig(
        format=config.logformat,
        level=int(settings["MOX_LOG_LEVEL"]),
        filename=settings["MOX_LOG_FILE"],
    )
    logger = logging.getLogger(config.loggername)
    logger.setLevel(int(settings["MOX_LOG_LEVEL"]))

    if settings["OS2SYNC_USE_LC_DB"]:
        engine = lcdb_os2mo.get_engine()
        session = lcdb_os2mo.get_session(engine)
        os2mo.get_sts_user = partial(lcdb_os2mo.get_sts_user, session)
        os2mo.get_sts_orgunit = partial(lcdb_os2mo.get_sts_orgunit, session)

    prev_date = datetime.datetime.now() - datetime.timedelta(days=1)
    hash_cache_file = pathlib.Path(settings["OS2SYNC_HASH_CACHE"])

    if hash_cache_file.exists():
        prev_date = datetime.datetime.fromtimestamp(hash_cache_file.stat().st_mtime)
    prev_date = prev_date.strftime("%Y-%m-%d")

    counter = collections.Counter()
    logger.info("mox_os2sync starting")
    log_mox_config(settings)

    if hash_cache_file and hash_cache_file.exists():
        os2sync.hash_cache.update(json.loads(hash_cache_file.read_text()))

    if not settings["OS2MO_ORG_UUID"]:
        settings["OS2MO_ORG_UUID"] = os2mo.os2mo_get("{BASE}/o/").json()[0]["uuid"]
    settings["OS2MO_HAS_KLE"] = os2mo.has_kle()

    orgunit_uuids = sync_os2sync_orgunits(settings, counter, prev_date)
    sync_os2sync_users(settings, orgunit_uuids, counter, prev_date)

    if hash_cache_file:
        hash_cache_file.write_text(json.dumps(os2sync.hash_cache, indent=4))

    log_mox_counters(counter)
    log_mox_config(settings)
    logger.info("mox_os2sync done")


if __name__ == "__main__":
    settings = config.settings
    main(settings)
