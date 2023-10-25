import datetime

from deepdiff.diff import DeepDiff
from sql_export.gql_lora_cache_async import get_gql_cache_settings
from sql_export.gql_lora_cache_async import GQLLoraCache
from sql_export.gql_lora_cache_async import GqlLoraCacheSettings
from sql_export.log import get_logger
from sql_export.log import LogLevel
from sql_export.log import setup_logging
from sql_export.old_lora_cache import OldLoraCache as LoraCache

"""Integration endpoints."""

from fastapi import APIRouter
from fastapi import BackgroundTasks

import urllib.error

import prometheus_client
from prometheus_client import CollectorRegistry
from prometheus_client import Gauge

logger = get_logger()
trigger_equiv_router = APIRouter()
from_date = "from_date"
to_date = "to_date"


# The addresses hava a bug in the old cache. The old cache takes the dar field beskrivelse,
# and uses that as the value. It does this in a loop, where it fills out the values. Sometimes the
# field is empty. In those cases the value is set to the same as the last iteration, as
# the loop doesn't reset the variables
def fix_addresses(old_addresses: dict, new_addresses: dict):
    for key, list_of_new_values in new_addresses.items():
        list_of_old_values = old_addresses.get(key, [])
        if len(list_of_old_values) == 0:
            continue

        for i in range(len(list_of_new_values)):
            new_address = list_of_new_values[i].copy()
            old_address = list_of_old_values[i]
            if new_address.get("scope") == "DAR":
                old_address["value"] = new_address["value"]

    return old_addresses


# The old cache has a problem where it-connections, managers, and org units never ends
# This means that a closed it connection would still be part of the actual state export
# though it shouldn't be.
def fix_never_ending(old_cache: dict, new_cache: dict[str, list[dict]]) -> dict:
    keys_to_pop = []
    for key, list_of_old_values in old_cache.items():
        if key not in new_cache:
            keys_to_pop.append(key)
            continue
        list_of_new_values: list[dict] = new_cache.get(key, []).copy()

        # list of checked elements that doesn't have the bug
        fixed_list: list[dict] = []

        for i in range(len(list_of_new_values)):
            old = list_of_old_values[i].copy()
            new = list_of_new_values[i]

            old[from_date] = new[from_date]
            old[to_date] = new[to_date]
            fixed_list.append(old)

        old_cache[key] = fixed_list

    for k in keys_to_pop:
        old_cache.pop(k)

    return old_cache


# MO this makes me sad, so many was to be infinity
def is_technically_none(date: str | None) -> bool:
    if date is None or len(date) == 0:
        return True

    d = datetime.datetime.fromisoformat(date)

    if d.year == 9999 or d.year <= 1930:
        return True

    return False


# Vi har lavet nogle ændringer i hvordan vi håndterer datoer når de kommer ud af graphql.
# Før tjekkede vi om tiden på datoen var midnat, hvis ikke den var det - f.eks hvis den
# var 3.4.2020 kl 23.59.59, så erstattede vi tidpunktet med kl 00.00.00. Hvilket vil
# sige at den 3.4.2020 23.59.59 er, alt efter hvordan vi henter det ud, det samme
# som den 3.4.2020 00.00.00. Det vil sige en from date kan afvige med op til ca 24
# timer.
#
# Det næste vi gjorde var så at trække en hel dag fra to_date, det er endnu 24 timer.
#
# Det gamle lora api gør stadig sådan, men det gør graphql ikke, og derfor kan der
# være en timediff på op til 47 timer, 59 min, 59 sek, og næsten et helt sekund
# i millisekunder
#
# Jeg kan ikke lide det, det er noget lort, men så længe vi ikke har lavet den
# opgave hvor vi grundlæggende ændrer på om vi bruger timezones så tror jeg at
# det her er nødvendigt.
def is_same_date(old_date: str | None, new_date: str | None) -> bool:
    if is_technically_none(old_date):
        return is_technically_none(new_date)
    if is_technically_none(new_date):
        return is_technically_none(old_date)

    # mypy gets angry if we don't do this
    assert old_date is not None
    assert new_date is not None

    od = datetime.datetime.fromisoformat(old_date)
    nd = datetime.datetime.fromisoformat(new_date)

    diff = abs(od - nd)

    return diff <= datetime.timedelta(days=2)


def handle_validities(old_cache: dict, new_cache: dict):

    # assume all new items are in the old as well, otherwise we will catch it later
    for nk, list_of_new_values in new_cache.items():
        if not isinstance(list_of_new_values, list):
            break

        list_of_old_values = old_cache[nk]

        length = len(list_of_new_values)

        for i in range(length):
            new_elem = list_of_new_values[i]
            old_elem = list_of_old_values[i]

            if is_same_date(old_elem[from_date], new_elem[from_date]):
                old_elem[from_date] = new_elem[from_date]

            if is_same_date(old_elem[to_date], new_elem[to_date]):
                old_elem[to_date] = new_elem[to_date]

    return old_cache, new_cache


def account_for_fixes(old_cache: LoraCache, new_cache: GQLLoraCache):
    old_cache.addresses = fix_addresses(old_cache.addresses, new_cache.addresses)
    old_cache.addresses = fix_never_ending(old_cache.addresses, new_cache.addresses)
    old_cache.units = fix_never_ending(old_cache.units, new_cache.units)
    old_cache.it_connections = fix_never_ending(
        old_cache.it_connections, new_cache.it_connections
    )
    old_cache.managers = fix_never_ending(old_cache.managers, new_cache.managers)
    old_cache.kles = fix_never_ending(old_cache.kles, new_cache.kles)
    old_cache.roles = fix_never_ending(old_cache.roles, new_cache.roles)
    old_cache.associations = fix_never_ending(
        old_cache.associations, new_cache.associations
    )

    return old_cache, new_cache


def are_caches_equivalent(
    old_cache: dict, new_cache: dict, do_deepdiff: bool = True, name: str | None = None
) -> bool:
    if name == "associations":
        old_cache = format_assocs(old_cache)
        new_cache = format_assocs(new_cache)

    if old_cache == new_cache:
        return True

    if do_deepdiff:
        logger.debug(80 * "#")
        logger.debug(f"cache = {name}")
        diff = DeepDiff(old_cache, new_cache, verbose_level=2)
        logger.debug(diff)
        print(80 * "!")
    return False


# The old cache did not calculate primary for historic caches
def remove_primary(engagements: dict):
    for key, value in engagements.items():
        for elem in value:
            elem.pop("primary_boolean")
    return engagements


def format_assocs(assoc_cache: dict):
    for assoc_key, assoc_list in assoc_cache.items():
        for assoc in assoc_list:
            if "unit" in assoc:
                assoc.pop("unit")

            if assoc.get("user") == "":
                assoc["user"] = None

    return assoc_cache


def compare_for_equivalence(
    old_cache: LoraCache, new_cache: GQLLoraCache, state: str
) -> bool:
    old_cache, new_cache = account_for_fixes(old_cache, new_cache)
    do_deepdiff = new_cache.settings.log_level == LogLevel.DEBUG

    if state == "Actual_State":
        old_cache.engagements = remove_primary(old_cache.engagements)

    new_cache.engagements = remove_primary(new_cache.engagements)

    cache_pairs = [
        (old_cache.facets, new_cache.facets, "facets"),
        (old_cache.classes, new_cache.classes, "classes"),
        (old_cache.users, new_cache.users, "users"),
        (old_cache.units, new_cache.units, "units"),
        (old_cache.addresses, new_cache.addresses, "addresses"),
        (old_cache.engagements, new_cache.engagements, "engagements"),
        (old_cache.managers, new_cache.managers, "managers"),
        (old_cache.associations, new_cache.associations, "associations"),
        (old_cache.leaves, new_cache.leaves, "leaves"),
        (old_cache.roles, new_cache.roles, "roles"),
        (old_cache.itsystems, new_cache.itsystems, "itsystems"),
        (old_cache.it_connections, new_cache.it_connections, "it_connections"),
        (old_cache.kles, new_cache.kles, "kles"),
        (old_cache.related, new_cache.related, "related"),
        # (old_cache.dar_cache, new_cache.dar_cache, "dar_cache"),
    ]

    equivalence_bools = []
    for old, new, name in cache_pairs:

        cons_old, cons_new = handle_validities(old_cache=old, new_cache=new)
        is_equiv = are_caches_equivalent(
            old_cache=cons_old, new_cache=cons_new, do_deepdiff=do_deepdiff, name=name
        )

        equivalence_bools.append((name, is_equiv))

    is_equivalent: bool = True
    for name, equal in equivalence_bools:
        if not equal:
            logger.debug("+++++++++++++++++++++++++++++++++++++++++")
            logger.debug(f"The first error is in {name}")
            logger.debug("+++++++++++++++++++++++++++++++++++++++++")
            is_equivalent = False

    return is_equivalent


def init_caches(settings: GqlLoraCacheSettings):

    new_cache_full_history = GQLLoraCache(
        resolve_dar=True, full_history=True, skip_past=False, settings=settings
    )
    new_cache_historic_no_past = GQLLoraCache(
        resolve_dar=True, full_history=True, skip_past=True, settings=settings
    )
    new_cache_actual_state = GQLLoraCache(
        resolve_dar=True, full_history=False, skip_past=False, settings=settings
    )

    old_cache_full_history = LoraCache(
        resolve_dar=True,
        full_history=True,
        skip_past=False,
        settings=settings.to_old_settings(),
    )
    old_cache_historic_no_past = LoraCache(
        resolve_dar=True,
        full_history=True,
        skip_past=True,
        settings=settings.to_old_settings(),
    )
    old_cache_actual_state = LoraCache(
        resolve_dar=True,
        full_history=False,
        skip_past=False,
        settings=settings.to_old_settings(),
    )

    return [
        (new_cache_actual_state, old_cache_actual_state, "Actual_State"),
        (new_cache_full_history, old_cache_full_history, "Full_History"),
        (new_cache_historic_no_past, old_cache_historic_no_past, "Historic_No_Past"),
    ]


def populate_caches(old_cache: LoraCache, new_cache: GQLLoraCache, state: str):
    logger.debug(80 * "=")
    logger.debug(f"Processing {state}")
    logger.debug(80 * "=")

    start = datetime.datetime.now()

    logger.debug("Populating the new cache")
    new_cache.populate_cache(dry_run=False)
    new_cache_time = datetime.datetime.now() - start
    logger.debug(f"Populated new cache in {new_cache_time}")

    logger.debug(80 * "+")
    logger.debug("Populating the old cache")
    start = datetime.datetime.now()

    old_cache.populate_cache(dry_run=False)
    old_cache.calculate_primary_engagements()
    old_cache.calculate_derived_unit_data()

    old_cache_time = datetime.datetime.now() - start
    logger.debug(f"Populated old cache in {old_cache_time}")
    logger.debug(80 * "+")

    logger.debug(f"Slowdown: {new_cache_time/old_cache_time}")

    return old_cache, new_cache


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


def test_cache_equivalence():
    settings = get_gql_cache_settings()
    setup_logging(settings.log_level.value)

    are_all_cache_states_equivalent: bool = True
    cache_pairs = init_caches(settings)
    for new_cache, old_cache, state in cache_pairs:
        are_all_cache_states_equivalent = True
        job = f"equivalence_test_{state}"
        notify_prometheus(settings=settings, job=job, start=True)
        try:
            old_cache, new_cache = populate_caches(
                old_cache=old_cache, new_cache=new_cache, state=state
            )
            is_equiv = compare_for_equivalence(
                old_cache=old_cache, new_cache=new_cache, state=state
            )
            if not is_equiv:
                are_all_cache_states_equivalent = False
        except Exception as e:
            notify_prometheus(settings=settings, job=job, error=True)
            raise e
        else:
            notify_prometheus(settings=settings, job=job, error=(not is_equiv))

    assert are_all_cache_states_equivalent


@trigger_equiv_router.post("/trigger_cache_equivalence")
async def trigger_cache_equivalence(
    background_tasks: BackgroundTasks,
) -> dict[str, str]:
    background_tasks.add_task(test_cache_equivalence())
    return {"triggered": "OK"}
