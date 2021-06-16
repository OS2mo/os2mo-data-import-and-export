import json
import logging
from typing import Dict
from typing import Optional

import click
from ra_utils.load_settings import load_settings
from tqdm import tqdm

from .ad_exceptions import CprNotFoundInADException
from .ad_exceptions import CprNotNotUnique
from .ad_exceptions import ManagerNotUniqueFromCprException
from .ad_exceptions import UserNotFoundException
from .ad_logger import start_logging
from .ad_reader import ADParameterReader
from .ad_writer import ADWriter
from exporters.sql_export.lora_cache import fetch_loracache


LOG_FILE = "mo_to_ad_sync.log"

logger = logging.getLogger("MoAdSync")


@click.command()
@click.option(
    "--lora-speedup/--no-lora-speedup",
    help="Utilize LoraCache to speedup the operation",
    is_flag=True,
    type=click.BOOL,
    default=lambda: load_settings()["integrations.ad_writer.lora_speedup"],
)
@click.option(
    "--mo-uuid-field",
    type=click.STRING,
    default=lambda: load_settings()["integrations.ad.write.uuid_field"],
)
@click.option(
    "--sync-cpr",
    help="Synchronize the specified user instead of all users",
    type=click.STRING,
)
@click.option(
    "--sync-username",
    help="Synchronize the specified user instead of all users",
    type=click.STRING,
)
@click.option("--ignore-occupied-names", is_flag=True, default=False)
def main(
    lora_speedup: bool,
    mo_uuid_field: str,
    sync_cpr: Optional[str],
    sync_username: Optional[str],
    ignore_occupied_names: bool,
):
    start_logging(LOG_FILE)

    lc, lc_historic = None, None
    if lora_speedup:
        lc, lc_historic = fetch_loracache()

    reader = ADParameterReader()
    writer = ADWriter(
        lc=lc,
        lc_historic=lc_historic,
        occupied_names=[] if ignore_occupied_names else None,
    )

    if sync_cpr or sync_username:
        print("Warning: --sync-cpr/--sync-username is for testing only")
        all_users = [reader.read_user(user=sync_username, cpr=sync_cpr)]
        # XXX: occupied_names should not be an empty array, but it takes forever to
        #      initalize, essentially reading all of AD.
        # TODO: We should support on-demand name generation without pre-seed.
    else:
        all_users = reader.read_it_all(print_progress=True)

    logger.info("Will now attempt to sync {} users".format(len(all_users)))

    def filter_missing_uuid_field(user):
        if mo_uuid_field.lower() in set(k.lower() for k in user):
            msg = "User {} does not have a {} field - skipping"
            logger.info(msg.format(user["SamAccountName"], mo_uuid_field))
            return False
        return True

    def update_stats(stats: Dict[str, int], response) -> Dict[str, int]:
        if response[0]:
            stats["fully_synced"] += 1
            if response[1] == "Sync completed":
                stats["updated"] += 1
                if response[2] is False:
                    stats["no_manager"] += 1

            if response[1] == "Nothing to edit":
                stats["nothing_to_edit"] += 1
                if response[2] is False:
                    stats["no_manager"] += 1
        else:
            if response[1] == "No active engagments":
                stats["no_active_engagement"] += 1
            else:
                stats["unknown_failed_sync"] += 1
        return stats

    stats = {
        "attempted_users": 0,
        "fully_synced": 0,
        "nothing_to_edit": 0,
        "updated": 0,
        "no_manager": 0,
        "unknown_manager_failure": 0,
        "cpr_not_unique": 0,
        "user_not_in_mo": 0,
        "user_not_in_ad": 0,
        "critical_error": 0,
        "unknown_failed_sync": 0,
        "no_active_engagement": 0,
    }
    all_users = list(filter(filter_missing_uuid_field, all_users))
    for user in tqdm(all_users, unit="user"):
        stats["attempted_users"] += 1

        msg = "Now syncing: {}, {}".format(user["SamAccountName"], user[mo_uuid_field])
        logger.info(msg)
        try:
            if sync_cpr or sync_username:
                response = writer.sync_user(user[mo_uuid_field], ad_dump=None)
            else:
                response = writer.sync_user(user[mo_uuid_field], ad_dump=all_users)
            logger.debug("Respose to sync: {}".format(response))
            stats = update_stats(stats, response)
        except ManagerNotUniqueFromCprException:
            stats["unknown_manager_failure"] += 1
            msg = "Did not find a unique manager for {}".format(user[mo_uuid_field])
            logger.error(msg)
        except CprNotNotUnique:
            stats["cpr_not_unique"] += 1
            msg = "User {} with uuid: {} has more than one AD account"
            logger.error(msg.format(user["Name"], user[mo_uuid_field]))
        except CprNotFoundInADException:
            stats["user_not_in_ad"] += 1
            msg = "User {}, {} with uuid {} could not be found by cpr"
            logger.error(
                msg.format(user["SamAccountName"], user["Name"], user[mo_uuid_field])
            )
        except UserNotFoundException:
            stats["user_not_in_mo"] += 1
            msg = "User {}, {} with uuid {} was not found i MO, unable to sync"
            logger.error(
                msg.format(user["SamAccountName"], user["Name"], user[mo_uuid_field])
            )
        except Exception as e:
            stats["critical_error"] += 1
            logger.error("Unhandled exception: {}".format(e))
            logger.exception("Unhandled exception:")
            print("Unhandled exception: {}".format(e))

    print()
    print(json.dumps(stats, indent=4))
    logger.info("Stats: {}".format(stats))


if __name__ == "__main__":
    main()
