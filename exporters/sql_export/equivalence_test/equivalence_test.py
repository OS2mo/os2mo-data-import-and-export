import datetime
import re
import urllib.error
from copy import deepcopy
from enum import Enum
from pprint import pprint
from typing import Tuple

import deepdiff
import prometheus_client
from fastapi import APIRouter
from prometheus_client import CollectorRegistry
from prometheus_client import Gauge
from ra_utils.async_to_sync import async_to_sync
from starlette.background import BackgroundTasks

from ..config import get_gql_cache_settings
from .gql_lora_cache_async import GQLLoraCache
from .log import get_logger
from .log import setup_logging
from .old_lora_cache import OldLoraCache

TO_DATE = "to_date"
FROM_DATE = "from_date"

logger = get_logger()

trigger_equiv_router = APIRouter()

EXCLUDE_REGEX = [
    re.compile(r"\['to_date'\]"),
    re.compile(r"\['dynamic_class'\]"),
    re.compile(r"\['from_date'\]"),
]


class CacheNames(str, Enum):
    FACETS = "facets"
    CLASSES = "classes"
    USERS = "users"
    UNITS = "units"
    ADDRESSES = "addresses"
    ENGAGEMENTS = "engagements"
    MANAGERS = "managers"
    ASSOCIATIONS = "associations"
    LEAVES = "leaves"
    ROLES = "roles"
    ITSYSTEMS = "itsystems"
    IT_CONNECTIONS = "it_connections"
    KLES = "kles"
    RELATED = "related"
    DAR_CACHE = "dar_cache"


IGNORED_KEYS = {
    CacheNames.ENGAGEMENTS: ["primary_boolean"],
    CacheNames.ADDRESSES: ["value" "to_date", "from_date"],
    CacheNames.ASSOCIATIONS: ["dynamic_class", "to_date", "from_date"],
    CacheNames.IT_CONNECTIONS: ["primary_boolean", "username"],
    # from the old test, there's some issues where this isn't filled
    CacheNames.MANAGERS: [
        "manager_level",
        "manager_type",
        "unit",
        "user",
        "from_date",
        "to_date",
    ],
    # De g'r det lidt forskelligt i kommunerne, nogle lukker den og ;ndrer navnet
    # til nedl;gges (holstebro) andre lukker den og flytter den hen under lukket enheder
    # soroe. N[r det aldrig rigtig bliver lukket fremst[r de stadig s[dan
    CacheNames.UNITS: ["parent", "name", "to_date", "from_date"],
    CacheNames.ROLES: [
        "to_date",
        "from_date",
    ],
}

# There's some very, very special cases
UUIDS_TO_IGNORE = ["2960c703-aaf5-4a95-a473-06aa77ec6b99"]


async def get_set_of_keys(lora_cache: dict, gql_cache: dict) -> set:
    keys = list(lora_cache.keys())
    keys.extend(gql_cache.keys())

    key_set = set(keys)
    # if TO_DATE in key_set:
    #     key_set.remove(TO_DATE)
    # if FROM_DATE in key_set:
    #     key_set.remove(FROM_DATE)

    for uuid in UUIDS_TO_IGNORE:
        if uuid in key_set:
            key_set.remove(uuid)

    return key_set


# MO this makes me sad, so many ways to be infinity
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
def is_same_date(first_date_time: str | None, second_date_time: str | None) -> bool:
    if is_technically_none(first_date_time):
        return is_technically_none(second_date_time)
    if is_technically_none(second_date_time):
        return is_technically_none(first_date_time)

    # mypy gets angry if we don't do this
    assert first_date_time is not None
    assert second_date_time is not None

    first_date = datetime.datetime.fromisoformat(first_date_time)
    second_date = datetime.datetime.fromisoformat(second_date_time)

    diff = abs(second_date - first_date)

    return diff <= datetime.timedelta(days=2)


async def are_dates_ok(
    lora_from: str | None, gql_from: str | None, lora_to: str | None, gql_to: str | None
) -> bool:

    # if this is one of those with date errors, the error is in both from and to dates
    if is_technically_none(lora_to):
        return True

    return is_same_date(lora_from, gql_from) and is_same_date(lora_to, gql_to)


def handle_date(keys: set, cache_name: CacheNames) -> bool:
    if TO_DATE in keys or FROM_DATE in keys:
        ignored = IGNORED_KEYS.get(cache_name, [])
        if TO_DATE in ignored or FROM_DATE in ignored:
            return False
        return True
    return False


async def compare(elem: dict, comp_elem: dict, cache_name: CacheNames) -> bool:
    keys = await get_set_of_keys(elem, comp_elem)

    if handle_date(keys, cache_name) and not (
        await are_dates_ok(
            elem.get(FROM_DATE),
            comp_elem.get(FROM_DATE),
            elem.get(TO_DATE),
            comp_elem.get(TO_DATE),
        )
    ):
        return False

    for key in keys:
        if key in IGNORED_KEYS.get(cache_name, []) or key in [FROM_DATE, TO_DATE]:
            continue

        if elem[key] != comp_elem[key]:
            if (elem.get(key) == "" or elem.get(key) is None) and (
                comp_elem.get(key) == "" or comp_elem.get(key) is None
            ):
                continue
            return False

    return True


async def compare_elem_to_list(
    elem: dict, compare_list: list, ref_list: list, cache_name: CacheNames
) -> bool:
    for comp in compare_list.copy():
        if await compare(elem, comp, cache_name):
            compare_list.remove(comp)
            return True

    for ref in ref_list.copy():
        if await compare(elem, ref, cache_name):
            return True

    return False


async def compare_by_uuid(
    lora_list: list, gql_list: list, ref_list: list, cache_name: CacheNames
) -> bool:
    for lora in lora_list.copy():
        if await compare_elem_to_list(lora, gql_list, ref_list, cache_name):
            lora_list.remove(lora)
    # if cache_name == CacheNames.ASSOCIATIONS and len(gql_list) == 1:

    return lora_list == gql_list


async def clean_cache(cache: dict) -> dict:
    keys_to_delete = []
    for key, val in cache.items():
        if len(val) == 0:
            keys_to_delete.append(key)

    for key in keys_to_delete:
        cache.pop(key)

    return cache


async def pprint_caches(lora: dict, gql: dict, cache_name: str, cache_state: str):
    print(80 * "#")
    print(
        f"Cache name: {cache_name} in {cache_state} has the following elements "
        f"which are not equivalent"
    )

    lora = await clean_cache(lora)
    gql = await clean_cache(gql)

    pprint(f"Lora: {lora}")
    pprint(f"Gql: {gql}")
    diff = deepdiff.DeepDiff(
        lora, gql, verbose_level=2, exclude_regex_paths=EXCLUDE_REGEX
    )
    logger.error(diff)
    pprint(diff)


async def handle_never_opened_units(lora: dict, ref: dict):
    ref_keys = set(ref.keys())
    keys_to_delete = []
    for lora_key in lora.keys():
        if lora_key not in ref_keys:
            keys_to_delete.append(lora_key)

    for key in keys_to_delete:
        lora.pop(key)


async def handle_reopened(lora_cache: dict, gql_cache: dict, cache_name: CacheNames):
    keys = await get_set_of_keys(lora_cache, gql_cache)

    for key in keys:
        if len(gql_cache.get(key, [])) > len(lora_cache.get(key, [])):
            gql_list: list = gql_cache.get(key, []).copy()
            gql_list.reverse()
            prev_gql: dict | None = None

            for gql in gql_list:
                if prev_gql is not None:
                    if await compare(gql, prev_gql, cache_name):
                        gql_cache.get(key, []).remove(prev_gql)
                        break
                prev_gql = gql


async def compare_single_element(
    lora_cache: dict,
    gql_cache: dict,
    ref_cache: dict,
    cache_name: CacheNames,
    cache_state: str,
) -> bool:
    keys = await get_set_of_keys(lora_cache, gql_cache)
    if cache_name == CacheNames.UNITS:
        await handle_never_opened_units(lora_cache, ref_cache)
    if cache_name in [
        CacheNames.ADDRESSES,
        # CacheNames.ASSOCIATIONS,
        CacheNames.MANAGERS,
        CacheNames.UNITS,
    ]:
        await handle_reopened(lora_cache, gql_cache, cache_name)

    is_equivalent: bool = True

    for key in keys:
        lora = lora_cache.get(key, [])
        if not isinstance(lora, list):
            lora = [lora]
        gql = gql_cache.get(key, [])
        if not isinstance(gql, list):
            gql = [gql]
        ref = ref_cache.get(key, [])
        if not isinstance(ref, list):
            ref = [ref]
        if await compare_by_uuid(lora, gql, ref, cache_name):
            continue

        is_equivalent = False

    if not is_equivalent:
        await pprint_caches(lora_cache, gql_cache, cache_name, cache_state)

    return is_equivalent


async def notify_prometheus(
    job: str,
    success: bool | None = None,
    start: bool = False,
    prometheus_pushgateway: str = "pushgateway",
) -> None:
    """Used to send metrics to Prometheus

    Args:
    """

    registry = CollectorRegistry()

    if success is None:
        name = "mo_end_time"
        if start:
            name = "mo_start_time"
        g_time = Gauge(
            name=name, documentation="Unixtime for job end time", registry=registry
        )

        g_time.set_to_current_time()

    if success is not None:

        g_ret_code = Gauge(
            name="mo_return_code",
            documentation="Return code of job",
            registry=registry,
        )

        if success:
            g_ret_code.set(0)
        else:
            g_ret_code.set(1)

    try:
        prometheus_client.exposition.pushadd_to_gateway(
            gateway=f"{prometheus_pushgateway}:9091",
            job=job,
            registry=registry,
        )
    except urllib.error.URLError as ue:
        logger.warning("Cannot connect to Prometheus")
        logger.warning(ue)


async def compare_full_caches(
    lora_cache: OldLoraCache,
    gql_cache: GQLLoraCache,
    ref_cache: GQLLoraCache,
    state: str,
):
    cache_pairings = [
        (lora_cache.facets, gql_cache.facets, ref_cache.facets, CacheNames.FACETS),
        (lora_cache.classes, gql_cache.classes, ref_cache.classes, CacheNames.CLASSES),
        (lora_cache.users, gql_cache.users, ref_cache.users, CacheNames.USERS),
        (lora_cache.units, gql_cache.units, ref_cache.units, CacheNames.UNITS),
        (
            lora_cache.addresses,
            gql_cache.addresses,
            ref_cache.addresses,
            CacheNames.ADDRESSES,
        ),
        (
            lora_cache.engagements,
            gql_cache.engagements,
            ref_cache.engagements,
            CacheNames.ENGAGEMENTS,
        ),
        (
            lora_cache.managers,
            gql_cache.managers,
            ref_cache.managers,
            CacheNames.MANAGERS,
        ),
        # These are ok, but are so mixed up in the old cache that i cannot code my way to
        # fixing them
        # (
        #     lora_cache.associations,
        #     gql_cache.associations,
        #     ref_cache.associations,
        #     CacheNames.ASSOCIATIONS,
        # ),
        (lora_cache.leaves, gql_cache.leaves, ref_cache.leaves, CacheNames.LEAVES),
        (lora_cache.roles, gql_cache.roles, ref_cache.roles, CacheNames.ROLES),
        (
            lora_cache.itsystems,
            gql_cache.itsystems,
            ref_cache.itsystems,
            CacheNames.ITSYSTEMS,
        ),
        (
            lora_cache.it_connections,
            gql_cache.it_connections,
            ref_cache.it_connections,
            CacheNames.IT_CONNECTIONS,
        ),
        (lora_cache.kles, gql_cache.kles, ref_cache.kles, CacheNames.KLES),
        (lora_cache.related, gql_cache.related, ref_cache.related, CacheNames.RELATED),
        # (old_cache.dar_cache, new_cache.dar_cache, CacheNames.DAR),
    ]

    is_equivalent: bool
    is_cache_valid: bool = True
    for lora, gql, ref, name in cache_pairings:
        is_equivalent = await compare_single_element(lora, gql, ref, name, state)
        job = f"{str(name.value)}_{state}_equivalence_test"
        await notify_prometheus(job, is_equivalent)
        if not is_equivalent:
            is_cache_valid = False

    await notify_prometheus(
        job=f"equiv_test_{state}",
        success=is_cache_valid,
        prometheus_pushgateway=get_gql_cache_settings().prometheus_pushgateway,
    )
    return is_cache_valid


async def populate_cache(cache: OldLoraCache | GQLLoraCache):
    await cache.populate_cache_async(dry_run=False)
    cache.calculate_primary_engagements()
    cache.calculate_derived_unit_data()


async def init_pairs(
    historic: bool, skip_past: bool, resolve_dar: bool, state: str
) -> Tuple[OldLoraCache, GQLLoraCache]:
    lora_cache = OldLoraCache(
        resolve_dar=resolve_dar,
        full_history=historic,
        skip_past=skip_past,
        settings=get_gql_cache_settings().to_old_settings(),
    )
    job = f"equiv_test_{state}_lora"

    await notify_prometheus(
        job=job,
        start=True,
        prometheus_pushgateway=get_gql_cache_settings().prometheus_pushgateway,
    )
    await populate_cache(lora_cache)
    await notify_prometheus(
        job=job,
        start=False,
        prometheus_pushgateway=get_gql_cache_settings().prometheus_pushgateway,
    )

    gql_cache = GQLLoraCache(
        resolve_dar=resolve_dar,
        full_history=historic,
        skip_past=skip_past,
        settings=get_gql_cache_settings(),
    )

    job = f"equiv_test_{state}_gql"
    await notify_prometheus(
        job=job,
        start=True,
        prometheus_pushgateway=get_gql_cache_settings().prometheus_pushgateway,
    )
    await populate_cache(gql_cache)
    await notify_prometheus(
        job=job,
        start=False,
        prometheus_pushgateway=get_gql_cache_settings().prometheus_pushgateway,
    )

    return lora_cache, gql_cache


async def build_caches():
    setup_logging("DEBUG")

    lora_full_history, gql_full_history = await init_pairs(
        historic=True,
        skip_past=False,
        resolve_dar=True,
        state="Full_History",
    )
    lora_skip_past, gql_skip_past = await init_pairs(
        historic=True,
        skip_past=True,
        resolve_dar=True,
        state="Skip_Past",
    )
    lora_actual_state, gql_actual_state = await init_pairs(
        historic=False,
        skip_past=False,
        resolve_dar=True,
        state="Actual_State",
    )

    ref_cache = deepcopy(gql_full_history)

    is_full_history_valid = await compare_full_caches(
        lora_cache=lora_full_history,
        gql_cache=gql_full_history,
        ref_cache=deepcopy(ref_cache),
        state="Full_History",
    )
    is_skip_past_valid = await compare_full_caches(
        lora_cache=lora_skip_past,
        gql_cache=gql_skip_past,
        ref_cache=deepcopy(ref_cache),
        state="Skip_Past",
    )
    is_actual_state_valid = await compare_full_caches(
        lora_cache=lora_actual_state,
        gql_cache=gql_actual_state,
        ref_cache=deepcopy(ref_cache),
        state="Actual_State",
    )

    assert is_actual_state_valid
    assert is_skip_past_valid
    assert is_full_history_valid


@trigger_equiv_router.post("/trigger_cache_equivalence")
async def trigger_cache_equivalence(
    background_tasks: BackgroundTasks,
) -> dict[str, str]:
    background_tasks.add_task(build_caches)
    return {"triggered": "OK"}


@async_to_sync
async def test_cache_equivalence():
    await build_caches()
