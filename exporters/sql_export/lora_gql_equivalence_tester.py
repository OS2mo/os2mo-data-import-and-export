# SPDX-FileCopyrightText: Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
import logging
import typing
import urllib.error
from pprint import pprint

import prometheus_client
from deepdiff import DeepDiff
from more_itertools import first
from prometheus_client import CollectorRegistry
from prometheus_client import Gauge
from ra_utils.async_to_sync import async_to_sync

from .gql_lora_cache_async import GQLLoraCache
from .gql_lora_cache_async import GqlLoraCacheSettings
from .old_lora_cache import OldLoraCache as LoraCache

logger = logging.getLogger(__name__)


# This is an equivalency test, an integration test, and is thus not designed to
# be run as part of the ci-pipeline
class TestEquivalence:
    def __init__(self, settings):
        self.settings: GqlLoraCacheSettings = settings

    # The old lora cache had some bugs, this accounts for them when comparing.
    # History and end dates for units, it connections, and managers were bugged, and
    # so was some of the addresses
    def account_for_know_errors(
        self, old_cache: dict, new_cache: dict, cache_type: str
    ) -> dict:

        ref_cache = {}

        lc_gql = GQLLoraCache(
            resolve_dar=True, full_history=True, skip_past=False, settings=self.settings
        )

        if cache_type != "addresses":
            ref_cache = self.new_cache_helper(lc_gql, cache_type)

        if cache_type == "addresses":
            for k, ocl in old_cache.items():
                ncl = new_cache[k]
                for i in range(len(ocl)):
                    if ocl[i]["scope"] == "DAR" and ocl[i]["value"] is None:
                        ocl[i]["value"] = ncl[i]["value"]

            return old_cache

        modified_old_cache = {}

        for k, old_list in old_cache.items():
            #
            if k not in new_cache:
                continue

            new_list: typing.List[typing.Dict] = []
            if k not in ref_cache:
                pprint(ref_cache)
            ref_list = ref_cache[k]

            new_cache_list = new_cache[k]
            for old in old_list:
                if old in new_cache_list:
                    new_list.append(old)
                    continue
                for ref in ref_list:
                    old["to_date"] = ref["to_date"]
                    old["from_date"] = ref["from_date"]
                    if old in new_cache_list and old not in new_list:
                        new_list.append(old)

            if not new_list:
                continue

            modified_old_cache[k] = new_list

        return modified_old_cache

    # Used as an entry generate a single cache, and get it
    @async_to_sync
    async def new_cache_helper(self, lc, cache_name):
        async with lc._setup_gql_client() as session:
            lc.gql_client_session = session
            match cache_name:
                case "facets":
                    await lc._cache_lora_facets()
                    return lc.facets
                case "classes":
                    await lc._cache_lora_classes()
                    return lc.classes
                case "it_systems":
                    await lc._cache_lora_itsystems()
                    return lc.itsystems
                case "users":
                    await lc._cache_lora_users()
                    return lc.users
                case "units":
                    await lc._cache_lora_units()
                    return lc.units
                case "engagements":
                    await lc._cache_lora_engagements()
                    return lc.facets
                case "roles":
                    await lc._cache_lora_roles()
                    return lc.roles
                case "leaves":
                    await lc._cache_lora_leaves()
                    return lc.leaves
                case "it_connections":
                    await lc._cache_lora_it_connections()
                    return lc.it_connections
                case "kles":
                    await lc._cache_lora_kles()
                    return lc.kles
                case "related":
                    await lc._cache_lora_related()
                    return lc.related
                case "managers":
                    await lc._cache_lora_managers()
                    return lc.managers
                case "associations":
                    await lc._cache_lora_associations()
                    return lc.associations
                case "addresses":
                    await lc._cache_lora_address()
                    return lc.addresses

    # Test populate, making a complete task. Also used for performance testing
    def test_populate(self, historic: bool, skip_past: bool, resolve_dar: bool):

        old_cache = LoraCache(
            resolve_dar=resolve_dar,
            full_history=historic,
            skip_past=skip_past,
            settings=self.settings.to_old_settings(),
        )

        new_cache = GQLLoraCache(
            resolve_dar=resolve_dar,
            full_history=historic,
            skip_past=skip_past,
            settings=self.settings,
        )

        new_cache.populate_cache(dry_run=False, skip_associations=False)

        old_cache.populate_cache(dry_run=False, skip_associations=False)
        old_cache.calculate_derived_unit_data()

        facets_diff = DeepDiff(old_cache.facets, new_cache.facets)
        assert (
            not facets_diff
        ), f"Found diff in facets, showing first of {len(facets_diff)}: {first(facets_diff)}"
        classes_diff = DeepDiff(old_cache.classes, new_cache.classes)
        assert (
            not classes_diff
        ), f"Found diff in classes, showing first of {len(classes_diff)}: {first(classes_diff)}"
        itsystems_diff = DeepDiff(old_cache.itsystems, new_cache.itsystems)
        assert (
            not itsystems_diff
        ), f"Found diff in itsystems, showing first of {len(itsystems_diff)}: {first(itsystems_diff)}"

        user_diff = DeepDiff(old_cache.users, new_cache.users)

        assert (
            not user_diff
        ), f"Found diff in users, showing first of {len(user_diff)}: {first(user_diff)}"

        corrected_loracache_units = self.account_for_know_errors(
            old_cache.units, new_cache.units, "units"
        )
        unit_diff = DeepDiff(corrected_loracache_units, new_cache.units)

        assert (
            not unit_diff
        ), f"Found diff in units, showing first of {len(unit_diff)}: {first(unit_diff)}"
        # There is a bug in the handling of primary engagements
        # https://redmine.magenta-aps.dk/issues/57530
        engagement_diff = DeepDiff(
            old_cache.engagements,
            new_cache.engagements,
            verbose_level=2,
            exclude_regex_paths=r"\[\'primary_boolean'\]",
        )
        breakpoint()
        assert (
            not engagement_diff
        ), f"Found diff in engagements, showing first of {len(engagement_diff)}: {first(engagement_diff)}"
        roles_diff = DeepDiff(old_cache.roles, new_cache.roles)
        assert (
            not roles_diff
        ), f"Found diff in roles, showing first of {len(roles_diff)}: {first(roles_diff)}"
        leaves_diff = DeepDiff(old_cache.leaves, new_cache.leaves)
        assert (
            not leaves_diff
        ), f"Found diff in leaves, showing first of {len(leaves_diff)}: {first(leaves_diff)}"
        corrected_loracache_it_connections = self.account_for_know_errors(
            old_cache.it_connections, new_cache.it_connections, "it_connections"
        )
        it_connections_diff = DeepDiff(
            corrected_loracache_it_connections, new_cache.it_connections
        )
        assert (
            not it_connections_diff
        ), f"Found diff in it_connections, showing first of {len(it_connections_diff)}: {first(it_connections_diff)}"

        kles_diff = DeepDiff(old_cache.kles, new_cache.kles)
        assert (
            not kles_diff
        ), f"Found diff in kles, showing first of {len(kles_diff)}: {first(kles_diff)}"
        related_diff = DeepDiff(old_cache.related, new_cache.related)
        assert (
            not related_diff
        ), f"Found diff in related, showing first of {len(related_diff)}: {first(related_diff)}"
        corrected_loracache_managers = self.account_for_know_errors(
            old_cache.managers, new_cache.managers, "managers"
        )
        managers_diff = DeepDiff(corrected_loracache_managers, new_cache.managers)
        assert (
            not managers_diff
        ), f"Found diff in managers, showing first of {len(managers_diff)}: {first(managers_diff)}"

        associations_diff = DeepDiff(old_cache.associations, new_cache.associations)
        assert (
            not associations_diff
        ), f"Found diff in associations, showing first of {len(associations_diff)}: {first(associations_diff)}"
        corrected_loracache_addresses = self.account_for_know_errors(
            old_cache.addresses, new_cache.addresses, "addresses"
        )
        addresses_diff = DeepDiff(corrected_loracache_addresses, new_cache.addresses)
        assert (
            not addresses_diff
        ), f"Found diff in addresses, showing first of {len(addresses_diff)}: {first(addresses_diff)}"


def notify_prometheus(
    settings: GqlLoraCacheSettings,
    job: str,
    start: bool = False,
    error: bool = False,
) -> None:
    """Used to send metrics to Prometheus

    Args:
    """
    registry = CollectorRegistry()
    name = "mo_end_time"
    if start:
        name = "mo_start_time"

    g_time = Gauge(
        name=name, documentation="Unixtime for job end time", registry=registry
    )
    g_time.set_to_current_time()

    g_ret_code = Gauge(
        name="mo_return_code",
        documentation="Return code of job",
        registry=registry,
    )
    if not error:
        g_ret_code.set(0)
    else:
        g_ret_code.inc(1)

    try:
        prometheus_client.exposition.pushadd_to_gateway(
            gateway=f"{settings.prometheus_pushgateway}:9091",
            job=job,
            registry=registry,
        )
    except urllib.error.URLError as ue:
        logger.warning("Cannot connect to Prometheus")
        logger.warning(ue)


def cache_equivalence():
    settings = GqlLoraCacheSettings()
    settings.start_logging_based_on_settings()

    job_name = "cache_equivalence_actual_state"
    try:
        notify_prometheus(settings=settings, job=job_name, start=True)
        tester = TestEquivalence(settings)
        tester.test_populate(historic=False, skip_past=True, resolve_dar=True)
        notify_prometheus(settings=settings, job=job_name)
    except Exception as e:
        notify_prometheus(settings=settings, job=job_name, error=True)
        raise e

    job_name = "cache_equivalence_historic"
    try:
        notify_prometheus(settings=settings, job=job_name, start=True)
        tester = TestEquivalence(settings)
        tester.test_populate(historic=True, skip_past=False, resolve_dar=True)
        notify_prometheus(settings=settings, job=job_name)
    except Exception as e:
        notify_prometheus(settings=settings, job=job_name, error=True)
        raise e

    job_name = "cache_equivalence_historic_skip_past"
    try:
        notify_prometheus(settings=settings, job=job_name, start=True)
        tester = TestEquivalence(settings)
        tester.test_populate(historic=True, skip_past=True, resolve_dar=True)
        notify_prometheus(settings=settings, job=job_name)
    except Exception as e:
        notify_prometheus(settings=settings, job=job_name, error=True)
        raise e
