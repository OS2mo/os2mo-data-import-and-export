import logging
import uuid
from functools import lru_cache
from functools import partial
from functools import wraps
from operator import itemgetter
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple

import click
import sentry_sdk
from ra_utils.apply import apply
from ra_utils.catchtime import catchtime
from ra_utils.jinja_filter import create_filters
from ra_utils.lazy_dict import LazyDict
from ra_utils.lazy_dict import LazyEval
from ra_utils.lazy_dict import LazyEvalBare
from ra_utils.load_settings import load_settings
from ra_utils.tqdm_wrapper import tqdm

from .ad_exceptions import NoActiveEngagementsException
from .ad_exceptions import NoPrimaryEngagementException
from .ad_logger import start_logging
from .ad_reader import ADParameterReader
from .ad_writer import ADWriter
from .read_ad_conf_settings import injected_settings
from exporters.sql_export.lora_cache import LoraCache

logger = logging.getLogger("CreateAdUsers")
export_logger = logging.getLogger("export")

FilterFunction = Callable[[Tuple[Dict, Dict]], bool]


class AdLifeCycle:
    def __init__(
        self, read_from_cache: bool = True, skip_occupied_names_check: bool = False
    ) -> None:
        logger.info("AD Sync Started")
        self._settings = self._load_settings()

        self.roots = self._settings["integrations.ad.write.create_user_trees"]

        self.stats = self._gen_stats()

        self.create_filters = self._load_jinja_filters("create_filters")
        self.disable_filters = self._load_jinja_filters("disable_filters")

        self.ad_reader = self._get_adreader()

        # This is a potentially slow step (since it may read LoraCache)
        print("Retrive LoRa dump")
        with catchtime() as t:
            self.lc, self.lc_historic = self._update_lora_cache(dry_run=read_from_cache)
        print("Done with LoRa caching: {}".format(t()))

        # Create a set of users with engagements for faster filtering
        engagements = self.lc_historic.engagements.values()
        self.users_with_engagements = set(map(lambda eng: eng[0]["user"], engagements))

        print("Retrieve AD Writer name list")
        with catchtime() as t:
            self.ad_writer = self._get_adwriter(
                lc=self.lc,
                lc_historic=self.lc_historic,
                skip_occupied_names=skip_occupied_names_check,
                all_settings=injected_settings("ad_lifecycle_injected_settings"),
            )
        print("Done with AD Writer init: {}".format(t()))

        logger.debug("__init__() done")

    def _load_settings(self):
        return load_settings()

    def _load_jinja_filters(self, source: str) -> List[Callable]:
        seeded_create_filters = partial(
            create_filters, tuple_keys=("employee", "ad_object")
        )
        setting_name = f"integrations.ad.lifecycle.{source}"
        filter_templates = self._settings.get(setting_name, [])
        return [
            # Decorate each `filter_func` so it will log skipped users under
            # a name such as "create_filters_num_0", etc.
            self.log_skipped(f"{source}_num_{num}")(filter_func)
            for num, filter_func in enumerate(seeded_create_filters(filter_templates))
        ]

    def _get_adreader(self):
        reader = ADParameterReader()
        reader.cache_all(print_progress=True)
        return reader

    def _get_adwriter(self, **kwargs):
        return ADWriter(**kwargs)

    def log_skipped(self, filtername):
        """Return decorated version of a filter function taking a single
        `tup` arg, which is an `(employee, ad_object)` tuple.
        If the filter function returns `False`, store the result in the
        `stats["skipped"][filtername]` dictionary by the employee UUID.
        """

        def get_employee_name(employee):
            if "name" in employee:
                return " ".join(employee["name"])
            elif "navn" in employee:
                return employee["navn"]
            else:
                return "unknown"

        def decorator(f):
            @wraps(f)
            def wrapper(tup):
                # Call the filter function saving its status
                status = f(tup)
                if status is False:
                    skipped = self.stats.setdefault("skipped", {})
                    users = skipped.setdefault(filtername, {})
                    # Add user UUID to dictionary (name is used for the value)
                    employee = tup[0]
                    users[employee["uuid"]] = get_employee_name(employee)
                return status

            return wrapper

        return decorator

    def _update_lora_cache(self, dry_run: bool = True) -> Tuple[LoraCache, LoraCache]:
        """
        Read all information from AD and LoRa.
        :param dry_run: If True, LoRa dump will be read from cache.
        """
        lc = LoraCache(resolve_dar=True, full_history=False)
        lc.populate_cache(dry_run=dry_run, skip_associations=True)
        lc.calculate_derived_unit_data()
        lc.calculate_primary_engagements()

        lc_historic = LoraCache(resolve_dar=True, full_history=True, skip_past=True)
        lc_historic.populate_cache(dry_run=dry_run, skip_associations=True)

        return lc, lc_historic

    def _gen_stats(self) -> Dict[str, Any]:
        return {
            "critical_errors": 0,
            "engagement_not_found": 0,
            "created_users": 0,
            "disabled_users": 0,
            "already_in_ad": 0,
            "no_active_engagements": 0,
            "not_in_user_tree": 0,
            "create_filtered": 0,
            "users": set(),
        }

    @apply
    def _find_user_unit_tree(self, user: dict, ad_object: dict) -> bool:
        try:
            (
                employment_number,
                title,
                eng_org_unit_uuid,
                eng_uuid,
            ) = self.ad_writer.datasource.find_primary_engagement(user["uuid"])
        except (NoActiveEngagementsException, NoPrimaryEngagementException):
            logger.warning(
                "Warning: Unable to find primary for {}!".format(user["uuid"])
            )
            return False

        logger.debug("Primary found, now find org unit location")

        try:
            unit = self.lc.units[eng_org_unit_uuid][0]
        except KeyError:
            logger.warning(
                "cannot find unit %r (user=%r)", eng_org_unit_uuid, user["uuid"]
            )
            return False

        # Walk up the organisation unit tree, starting at `unit["parent"]`.
        # Stop when we find an allowed root node, or if we encounter a node
        # without a parent (must be root?)
        looking = True
        while looking:
            if unit["uuid"] in self.roots:
                return True
            if unit["parent"] is None:
                return False

            if unit["parent"] in self.lc.units:
                unit = self.lc.units[unit["parent"]][0]
            else:
                logger.warning(
                    "cannot find parent unit %r (user=%r)", unit["parent"], user["uuid"]
                )
                looking = False

        return False

    def _gen_filtered_employees(
        self, in_filters: Optional[List[FilterFunction]] = None
    ):
        def enrich_with_ad_user(mo_employee: dict) -> Tuple[Dict, Dict]:
            """Enrich mo_employee with AD employee dictionary."""
            cpr = mo_employee["cpr"]
            ad_object = self.ad_reader.read_user(cpr=cpr, cache_only=True)
            return mo_employee, ad_object

        @lru_cache(maxsize=0)
        def get_engagements() -> List[LazyDict]:
            """Produce a list of engagements with lazily evaluated properties."""

            def make_class_lazy(class_attribute: str, mo_engagement: dict) -> dict:
                """Create a lazily evaluated class property."""
                class_uuid = mo_engagement[class_attribute]
                mo_engagement[class_attribute + "_uuid"] = class_uuid
                mo_engagement[class_attribute] = LazyEvalBare(
                    lambda: {
                        **self.lc.classes[class_uuid],
                        "uuid": class_uuid,
                    }
                )
                return mo_engagement

            lc_engagements: List[List[Dict]] = self.lc.engagements.values()
            engagements: Iterator[Dict] = map(itemgetter(0), lc_engagements)
            lazy_engagements: Iterator[LazyDict] = map(LazyDict, engagements)
            enriched_engagements: Iterator[LazyDict] = map(
                # Enrich engagement_type class
                partial(make_class_lazy, "engagement_type"),
                map(
                    # Enrich primary_type class
                    partial(make_class_lazy, "primary_type"),
                    map(
                        # Enrich job_function class
                        partial(make_class_lazy, "job_function"),
                        lazy_engagements,
                    ),
                ),
            )
            return list(enriched_engagements)

        def enrich_with_engagements(mo_employee: dict) -> LazyDict:
            """Enrich mo_employee with lazy engagement information.

            The list of engagements is itself lazy, so this code is essentially free
            when it is not in use.
            """
            # Turn mo_employee into a lazy dict and add lazy properties
            lazy_employee: LazyDict = LazyDict(mo_employee)

            lazy_employee["engagements"] = LazyEvalBare(
                lambda: list(
                    filter(
                        lambda engagement: engagement["user"] == mo_employee["uuid"],
                        get_engagements(),
                    )
                )
            )

            lazy_employee["primary_engagement"] = LazyEval(
                lambda key, dictionary: next(
                    filter(
                        lambda engagement: engagement.get("primary_boolean", False),
                        dictionary["engagements"],
                    ),
                    None,
                )
            )

            return lazy_employee

        filters: List[FilterFunction] = in_filters or []

        lc_employees: List[List[Dict]] = self.lc.users.values()
        nonempty_employees = filter(lambda val: len(val) > 0, lc_employees)
        tqdm_employees: List[List[Dict]] = tqdm(nonempty_employees)
        # From employee_effects --> employees
        employees: Iterator[Dict] = map(itemgetter(0), tqdm_employees)

        # Enrich with engagements
        ee_employees: Iterator[Dict] = map(enrich_with_engagements, employees)

        # Enrich with ad_objects
        ad_employees: Iterator[Tuple[Dict, Dict]] = map(
            enrich_with_ad_user, ee_employees
        )

        # Apply requested filters
        for filter_func in filters:
            ad_employees = filter(filter_func, ad_employees)
        return ad_employees

    def disable_ad_accounts(self, dry_run: bool = False) -> Dict[str, Any]:
        """Iterate over all users and disable non-active AD accounts."""

        @apply
        def filter_user_not_in_ad(employee: dict, ad_object: dict) -> bool:
            in_ad = bool(ad_object)
            if not in_ad:
                logger.debug("User {} does not have an AD account".format(employee))
                return False
            return True

        @apply
        def filter_user_has_engagements(employee: dict, ad_object: dict) -> bool:
            # Check the user does not have a valid engagement:
            # TODO: Consider using the lazy properties for this
            if employee["uuid"] in self.users_with_engagements:
                logger.debug("User {} is active - do not touch".format(employee))
                return False
            return True

        employees = self._gen_filtered_employees(
            [
                # Remove users that does not exist in AD
                filter_user_not_in_ad,
                # Remove users that have active engagements
                filter_user_has_engagements,
            ]
            + self.disable_filters
        )
        # Employees now contain only employees which should be disabled
        for employee, ad_object in employees:
            logger.debug("This user has no active engagemens, we should disable")
            # This user has an AD account, but no engagements - disable
            sam = ad_object["SamAccountName"]
            status = True
            message = "dry-run"
            if not dry_run:
                status, message = self.ad_writer.enable_user(username=sam, enable=False)
            if status:
                logger.debug("Disabled: {}".format(sam))
                self.stats["disabled_users"] += 1
                self.stats["users"].add(employee["uuid"])
            else:
                logger.warning("enable_user call failed!")
                logger.warning(message)
                self.stats["critical_errors"] += 1

        return self.stats

    def create_ad_accounts(self, dry_run: bool = False) -> Dict[str, Any]:
        """Iterate over all users and create missing AD accounts."""

        @self.log_skipped("filter_user_already_in_ad")
        @apply
        def filter_user_already_in_ad(employee, ad_object):
            in_ad = bool(ad_object)
            if in_ad:
                self.stats["already_in_ad"] += 1
                logger.debug("User {} is already in AD".format(employee))
                return False
            return True

        @self.log_skipped("filter_user_without_engagements")
        @apply
        def filter_user_without_engagements(employee, ad_object):
            # TODO: Consider using the lazy properties for this
            if employee["uuid"] not in self.users_with_engagements:
                self.stats["no_active_engagements"] += 1
                logger.debug(
                    "User {} has no active engagements - skip".format(employee)
                )
                return False
            return True

        @self.log_skipped("filter_users_outside_unit_tree")
        def filter_users_outside_unit_tree(tup):
            status = self._find_user_unit_tree(tup)
            if status is False:
                self.stats["not_in_user_tree"] += 1
            return status

        def run_create_filters(tup):
            status = all(create_filter(tup) for create_filter in self.create_filters)
            if status is False:
                self.stats["create_filtered"] += 1
            return status

        employees = self._gen_filtered_employees(
            [
                # Remove users that already exist in AD
                filter_user_already_in_ad,
                # Remove users that have no active engagements at all
                filter_user_without_engagements,
                # Check if the user is in a create-user sub-tree
                filter_users_outside_unit_tree,
                # Run all create_filters
                run_create_filters,
            ]
        )
        # Employees now contain only employees which should be created
        for employee, ad_object in employees:
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
                    self.stats["created_users"] += 1
                    self.stats["users"].add(employee["uuid"])
                else:
                    logger.warning("create_user call failed!")
                    logger.warning(message)
                    self.stats["critical_errors"] += 1
            except NoPrimaryEngagementException:
                logger.exception("No engagment found!")
                self.stats["engagement_not_found"] += 1
            except Exception as e:
                logger.exception("Unknown error!")
                export_logger.error(
                    "Error creating AD user for MO user %r: %r",
                    employee["uuid"],
                    e,
                )
                self.stats["critical_errors"] += 1

        return self.stats


def write_stats(stats: Dict[str, Any]) -> None:
    logger.info("Stats: {}".format(stats))
    stats["users"] = "Written in log file"
    print(stats)


def run_preview_command_for_uuid(sync: AdLifeCycle, mo_uuid: str):
    commands = sync.ad_writer._preview_create_command(
        mo_uuid, ad_dump=None, create_manager=False
    )
    for cmd in commands:
        click.echo_via_pager(cmd)
    return commands


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
@click.option("--read-from-cache", is_flag=True, envvar="USE_CACHED_LORACACHE")
@click.option(
    "--skip-occupied-names-check",
    default=False,
    is_flag=True,
    help="Skip reading all current user names from AD. Only for testing!",
    type=click.BOOL,
)
@click.option(
    "--preview-command-for-uuid",
    help="Given a MO user UUID, preview the PowerShell command to be run",
    type=click.STRING,
)
def ad_life_cycle(
    create_ad_accounts: bool,
    disable_ad_accounts: bool,
    dry_run: bool,
    read_from_cache: bool,
    skip_occupied_names_check: bool,
    preview_command_for_uuid: Optional[uuid.UUID],
) -> None:
    """Create or disable users."""
    logger.debug(
        "Running ad_life_cycle with: {}".format(
            {
                "create_ad_accounts": create_ad_accounts,
                "disable_ad_accounts": disable_ad_accounts,
                "dry_run": dry_run,
                "read_from_cache": read_from_cache,
            }
        )
    )

    sync = AdLifeCycle(
        read_from_cache=read_from_cache,
        skip_occupied_names_check=skip_occupied_names_check,
    )

    if "crontab.SENTRY_DSN" in sync._settings:
        sentry_sdk.init(dsn=sync._settings["crontab.SENTRY_DSN"])

    if preview_command_for_uuid:
        run_preview_command_for_uuid(sync, str(preview_command_for_uuid))
        return

    if not any([create_ad_accounts, disable_ad_accounts]):
        raise click.ClickException(
            "Either create_ad_accounts or disable_ad_accounts must be given!"
        )

    if create_ad_accounts:
        stats = sync.create_ad_accounts(dry_run)
        write_stats(stats)

    if disable_ad_accounts:
        stats = sync.disable_ad_accounts(dry_run)
        write_stats(stats)


if __name__ == "__main__":
    start_logging("AD_life_cycle.log")
    ad_life_cycle()
