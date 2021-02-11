import json
import logging
import pathlib
from jinja2 import Template
from operator import itemgetter
from functools import wraps, partial, lru_cache

import click
from os2mo_helpers.mora_helpers import MoraHelper
from tqdm import tqdm

from exporters.sql_export.lora_cache import LoraCache
from integrations.ad_integration import ad_logger, ad_reader, ad_writer
from integrations.ad_integration.ad_exceptions import NoPrimaryEngagementException, NoActiveEngagementsException

logger = logging.getLogger("CreateAdUsers")


def apply(func):
    @wraps(func)
    def applied(tup):
        return func(*tup)

    return applied

def method_apply(method):
    @wraps(method)
    def applied(self, tup):
        return method(self, *tup)

    return applied


from time import perf_counter
from contextlib import contextmanager

@contextmanager
def catchtime() -> float:
    start = perf_counter()
    yield lambda: perf_counter() - start


from collections.abc import Mapping


class LazyEval:
    def __init__(self, cally, cache=True):
        self.cally = cally
        self.cache = cache

    def do_cache(self):
        return self.cache

    def __call__(self, dictionary):
        return self.cally(dictionary)


class LazyDict(Mapping):
    def __init__(self, *args, **kw):
        self._raw_dict = dict(*args, **kw)

    def __getitem__(self, key):
        value = self._raw_dict.__getitem__(key)
        # Check if we got back a LazyEval item
        if type(value) == LazyEval:
            return self._handle_lazy(key, value)
        return value

    def __setitem__(self, key, value):
        self._raw_dict.__setitem__(key, value)

    def _handle_lazy(self, key, lazy_eval):
        value = lazy_eval(self)
        if lazy_eval.do_cache():
            self._raw_dict[key] = value
        return value
 
    def __repr__(self):
        return repr(self._raw_dict)

    def __iter__(self):
        return iter(self._raw_dict)

    def __len__(self):
        return len(self._raw_dict)


#a = LazyDict({'a': 'b'})
#print(a)
#print(a['a'])
#print(a['a'])
#
#b = 0
#def counter_func():
#    global b
#    b = b + 1
#    return b
#
#
#a = LazyDict({'a': counter_func})
#print(a)
#print(a['a'])
#print(a['a'])
#
#a = LazyDict({'a': LazyEval(counter_func)})
#print(a)
#print(a['a'])
#print(a['a'])
#
#a = LazyDict({'a': LazyEval(counter_func, cache=False)})
#print(a)
#print(a['a'])
#print(a['a'])
#
#
#import sys
#sys.exit(1)


def create_filters(jinja_strings):
    def string_to_bool(v):
        return v.lower() in ("yes", "true", "t", "1")

    def as_filter(template):
        @apply
        def filter(employee, ad_object):
            result = template.render(employee=employee, ad_object=ad_object)
            return string_to_bool(result)
        return filter

    jinja_templates = map(Template, jinja_strings)
    filter_functions = map(as_filter, jinja_templates)
    return list(filter_functions)


class AdLifeCycle:
    def __init__(self, use_cached_mo=False):
        logger.info("AD Sync Started")
        cfg_file = pathlib.Path.cwd() / "settings" / "settings.json"
        if not cfg_file.is_file():
            raise Exception("No setting file")
        settings = json.loads(cfg_file.read_text())

        self.roots = settings["integrations.ad.write.create_user_trees"]
        self.create_filters = create_filters(settings.get(
            "integrations.ad.lifecycle.create_filters", []
        ))
        self.disable_filters = create_filters(settings.get(
            "integrations.ad.lifecycle.disable_filters", []
        ))
        
        # This is a slow step (since ADReader reads all users)
        print("Retrieve AD dump")
        with catchtime() as t:
            self.ad_reader = ad_reader.ADParameterReader()
            all_users = self.ad_reader.cache_all()
        print("Done with AD caching: {}".format(t()))
        occupied_names = set(map(itemgetter('SamAccountName'), all_users))

        # This is a potentially slow step (since it may read LoraCache)
        print("Retrive LoRa dump")
        with catchtime() as t:
            self._update_lora_cache(dry_run=use_cached_mo)
        print("Done with LoRa caching: {}".format(t()))

        # Create a set of users with engagements for faster filtering
        engagements = self.lc_historic.engagements.values()
        self.users_with_engagements = set(map(lambda eng: eng[0]["user"], engagements))

        # This is a slow step (since ADWriter reads all SAM names in __init__)
        print("Retrieve AD Writer name list")
        with catchtime() as t:
            self.ad_writer = ad_writer.ADWriter(lc=self.lc, lc_historic=self.lc_historic, occupied_names=occupied_names)
        print("Done with AD Writer init: {}".format(t()))

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

    @method_apply
    def _find_user_unit_tree(self, user, ad_object):
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

    def _gen_filtered_employees(self, filters=None):
        def enrich_with_ad_user(mo_employee):
            cpr = mo_employee['cpr']
            ad_object = self.ad_reader.read_user(cpr=cpr, cache_only=True)
            return mo_employee, ad_object

        @lru_cache(maxsize=0)
        def get_engagements():
            def make_class_lazy(class_attribute, mo_engagement):
                class_uuid = mo_engagement[class_attribute]
                mo_engagement[class_attribute + "_uuid"] = class_uuid
                mo_engagement[class_attribute] = LazyEval(lambda dictionary: {
                    **self.lc.classes[class_uuid],
                    "uuid": class_uuid,
                })
                return mo_engagement

            engagements = self.lc.engagements.values()
            engagements = map(itemgetter(0), engagements)
            engagements = map(LazyDict, engagements)
            engagements = map(partial(make_class_lazy, "engagement_type"), engagements)
            engagements = map(partial(make_class_lazy, "primary_type"), engagements)
            engagements = map(partial(make_class_lazy, "job_function"), engagements)
            return list(engagements)

        def enrich_with_engagements(mo_employee):
            # Turn mo_employee into a lazy dict and add lazy properties
            mo_employee = LazyDict(mo_employee)
            
            mo_employee["engagements"] = LazyEval(lambda dictionary: list(filter(
                lambda engagement: engagement["user"] == mo_employee["uuid"],
                get_engagements()
            )))

            mo_employee["primary_engagement"] = LazyEval(lambda dictionary: next(filter(
                lambda engagement: engagement.get("primary_boolean", False),
                dictionary["engagements"]
            ), None))

            return mo_employee

        filters = filters or []

        employees = self.lc.users.values()
        employees = tqdm(employees)
        # From employee_effects --> employees
        employees = map(itemgetter(0), employees)
        # Enrich with engagements
        employees = map(enrich_with_engagements, employees)
        # Enrich with ad_objects
        employees = map(enrich_with_ad_user, employees)
        # Apply requested filters
        for filter_func in filters:
            employees = filter(filter_func, employees)
        return employees

    def disable_ad_accounts(self):
        """Iterate over all users and disable non-active AD accounts."""

        @apply
        def filter_user_not_in_ad(employee, ad_object):
            in_ad = bool(ad_object)
            if not in_ad:
                logger.debug("User {} does not have an AD account".format(employee))
                return False
            return True

        @apply
        def filter_user_has_engagements(employee, ad_object):
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
            ] + self.disable_filters
        )
        # Employees now contain only employees which should be disabled
        for employee, ad_object in employees:
            logger.debug("This user has no active engagemens, we should disable")
            # This user has an AD account, but no engagements - disable
            cpr = employee["cpr"]
            sam = ad_object["SamAccountName"]
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

        @apply
        def filter_user_already_in_ad(employee, ad_object):
            in_ad = bool(ad_object)
            if in_ad:
                logger.debug("User {} is already in AD".format(employee))
                return False
            return True

        @apply
        def filter_user_without_engagements(employee, ad_object):
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
            ] + self.create_filters
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
                    #status, message = self.ad_writer.create_user(
                    #    employee["uuid"], create_manager=False
                    #)
                    print('CREATE USER ' + employee["uuid"])
                    pass
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
