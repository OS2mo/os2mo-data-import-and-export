import json
import logging
import pathlib
from itertools import starmap
from operator import itemgetter

import click
from os2mo_helpers.mora_helpers import MoraHelper

from exporters.sql_export.lora_cache import LoraCache
from integrations.ad_integration import ad_logger, ad_reader, ad_writer
from integrations.ad_integration.ad_exceptions import NoPrimaryEngagementException

logger = logging.getLogger("CreateAdUsers")


class AdLifeCycle:
    def __init__(self, use_cached_mo=False):
        logger.info("AD Sync Started")
        cfg_file = pathlib.Path.cwd() / "settings" / "settings.json"
        if not cfg_file.is_file():
            raise Exception("No setting file")
        settings = json.loads(cfg_file.read_text())

        self.roots = settings["integrations.ad.write.create_user_trees"]
        self.mora_base = settings["mora.base"]

        # This is a slow step (since ADReader reads all users)
        logger.info("Retrieve AD dump")
        self.ad_reader = ad_reader.ADParameterReader()
        self.ad_reader.cache_all()
        logger.info("Done with AD caching")

        # This is a potentially slow step (since it may read LoraCache)
        logger.info("Retrive LoRa dump")
        self._update_lora_cache(dry_run=use_cached_mo)
        logger.info("Done")

        # Create a set of users with engagements for faster filtering
        engagements = self.lc_historic.engagements.values()
        self.users_with_engagements = set(map(lambda eng: eng[0]["user"], engagements))

        # This is a slow step (since ADWriter reads all SAM names in __init__)
        logger.info("Retrieve AD Writer name list")
        self.ad_writer = ad_writer.ADWriter(lc=self.lc, lc_historic=self.lc_historic)
        logger.info("Done with AD Writer init")

        logger.debug("__init__() done")

    def _update_lora_cache(self, dry_run=False):
        """
        Read all information from AD and LoRa.
        :param dry_run: If True, LoRa dump will be read from cache.
        """
        self.lc = LoraCache(resolve_dar=False, full_history=False)
        self.lc.populate_cache(dry_run=dry_run, skip_associations=True)
        self.lc.calculate_derived_unit_data()
        self.lc.calculate_primary_engagements()
        self.lc_historic = LoraCache(
            resolve_dar=False, full_history=True, skip_past=True
        )
        self.lc_historic.populate_cache(dry_run=dry_run, skip_associations=True)

    def _gen_stats(self):
        return {
            "critical_errors": 0,
            "engagement_not_found": 0,
            "created_users": 0,
            "disabled_users": 0,
            "users": set(),
        }

    def _is_user_in_ad(self, employee):
        """Check if the given employee is found in AD."""
        cpr = employee["cpr"]
        response = self.ad_reader.read_user(cpr=cpr, cache_only=True)
        return bool(response)

    def _find_user_unit_tree(self, user):
        try:
            (
                employment_number,
                title,
                eng_org_unit_uuid,
                eng_uuid,
            ) = self.ad_writer.datasource.find_primary_engagement(user["uuid"])
        except NoActiveEngagementsException:
            logger.warning(
                "Warning: Unable to find primary for {}!".format(user["uuid"])
            )
            return False

        logger.debug("Primary found, now find org unit location")
        unit = self.lc.units[eng_org_unit_uuid][0]
        while True:
            if unit["uuid"] in self.roots:
                return True
            if unit["parent"] is None:
                return False
            unit = self.lc.units[unit["parent"]][0]

    def _gen_filtered_employees(self, filters):
        employees = self.lc.users.values()
        total_employees = len(employees)

        def print_progress(i, employee):
            i = i + 1
            logger.debug("Now testing ({}): {}".format(i, employee))
            if i % 1000 == 0 or i == total_employees:
                logger.info("Progress: {}/{}".format(i, total_employees))
                print("Progress: {}/{}".format(i, total_employees))
            return employee

        # From employee_effects --> employees
        employees = map(itemgetter(0), employees)
        # No-transform, called for side-effect
        employees = starmap(print_progress, enumerate(employees))
        # Apply requested filters
        for filter_func in filters:
            employees = filter(filter_func, employees)
        return employees

    def disable_ad_accounts(self):
        """Iterate over all users and disable non-active AD accounts."""

        def filter_user_not_in_ad(employee):
            in_ad = self._is_user_in_ad(employee)
            if not in_ad:
                logger.debug("User {} does not have an AD account".format(employee))
                return False
            return True

        def filter_user_has_engagements(employee):
            # Check the user does not have a valid engagement:
            if employee["uuid"] in self.users_with_engagements:
                logger.debug("User {} is active - do not touch".format(employee))
                return False
            return True

        stats = self._gen_stats()
        employees = self._gen_filtered_employees(
            [
                # Remove users that does not exist in AD
                filter_user_not_in_ad,
                # Remove users that have active engagements
                filter_user_has_engagements,
            ]
        )
        # Employees now contain only employees which should be disabled
        for employee in employees:
            logger.debug("This user has no active engagemens, we should disable")
            # This user has an AD account, but no engagements - disable
            cpr = employee["cpr"]
            response = self.ad_reader.read_user(cpr=cpr, cache_only=True)
            sam = response["SamAccountName"]
            status = True
            message = "dry-run"
            if not dry_run:
                status, message = self.ad_writer.enable_user(username=sam, enable=False)
            if status:
                logger.debug("Disabled: {}".format(sam))
                stats["disabled_users"] += 1
            else:
                logger.warning("enable_user call failed!")
                logger.warning(message)
                stats["critical_errors"] += 1

        return stats

    def create_ad_accounts(self, dry_run=False):
        """Iterate over all users and create missing AD accounts."""

        def filter_user_already_in_ad(employee):
            in_ad = self._is_user_in_ad(employee)
            if id_ad:
                logger.debug("User {} is already in AD".format(employee))
                return False
            return True

        def filter_user_without_engagements(employee):
            if employee["uuid"] not in self.users_with_engagements:
                logger.debug(
                    "User {} has no active engagements - skip".format(employee)
                )
                return False
            return True

        stats = self._gen_stats()
        employees = self._gen_filtered_employees(
            [
                # Remove users that already exist in AD
                filter_user_already_in_ad,
                # Remove users that have no active engagements at all
                filter_user_without_engagements,
                # Check if the user is in a create-user sub-tree
                self._find_user_unit_tree,
            ]
        )
        # Employees now contain only employees which should be created
        for employee in employees:
            logger.debug("Create account for {}".format(employee))
            try:
                # Create user without manager to avoid risk of failing
                # if manager is not yet in AD. The manager will be attached
                # by the next round of sync.
                status = True
                message = "dry-run"
                if not dry_run:
                    status, message = self.ad_writer.create_user(
                        employee["uuid"], create_manager=False
                    )
                if status:
                    logger.debug("New username: {}".format(message))
                    stats["created_users"] += 1
                    stats["users"].add(employee["uuid"])
                else:
                    logger.warning("create_user call failed!")
                    logger.warning(message)
                    stats["critical_errors"] += 1
            except NoPrimaryEngagementException:
                logger.error("No engagment found!")
                stats["engagement_not_found"] += 1
            except:
                logger.exception("Unknown error!")
                stats["critical_errors"] += 1

        return stats


def write_stats(stats):
    logger.info("Stats: {}".format(stats))
    stats["users"] = "Written in log file"
    print(stats)


@click.command()
@click.option(
    "--create-ad-accounts",
    default=False,
    is_flag=True,
    help="Create AD Users.",
    type=click.BOOL,
)
@click.option(
    "--disable-ad-accounts",
    default=False,
    is_flag=True,
    help="Disable AD Users.",
    type=click.BOOL,
)
@click.option(
    "--dry-run",
    default=False,
    is_flag=True,
    help="Dry-run without changes.",
    type=click.BOOL,
)
@click.option(
    "--use-cached-mo",
    default=False,
    is_flag=True,
    help="Use cached LoRa data, if false cache is refreshed.",
    type=click.BOOL,
)
def ad_life_cycle(create_ad_accounts, disable_ad_accounts, dry_run, use_cached_mo):
    """Create or disable users."""
    logger.debug(
        "Running ad_life_cycle with: {}".format(
            {
                "create_ad_accounts": create_ad_accounts,
                "disable_ad_accounts": disable_ad_accounts,
                "dry_run": dry_run,
                "use_cached_mo": use_cached_mo,
            }
        )
    )

    if not any([create_ad_accounts, disable_ad_accounts]):
        raise click.ClickException(
            "Either create_ad_accounts or disable_ad_accounts must be given!"
        )

    sync = AdLifeCycle(use_cached_mo=use_cached_mo)

    if create_ad_accounts:
        stats = sync.create_ad_accounts(dry_run)
        write_stats(stats)

    if disable_ad_accounts:
        stats = sync.disable_ad_accounts(dry_run)
        write_stats(stats)


if __name__ == "__main__":
    ad_logger.start_logging("AD_life_cycle.log")
    ad_life_cycle()
