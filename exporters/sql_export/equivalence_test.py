import datetime
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

from .config import get_gql_cache_settings
from .gql_lora_cache_async import GQLLoraCache
from .log import get_logger
from .log import setup_logging
from .old_lora_cache import OldLoraCache

TO_DATE = "to_date"
FROM_DATE = "from_date"

logger = get_logger()

trigger_equiv_router = APIRouter()


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
    CacheNames.ADDRESSES: ["value"],
}


async def get_set_of_keys(lora_cache: dict, gql_cache: dict) -> set:
    keys = list(lora_cache.keys())
    keys.extend(gql_cache.keys())

    return set(keys)


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


async def compare(elem: dict, comp_elem: dict, cache_name: CacheNames) -> bool:
    keys = await get_set_of_keys(elem, comp_elem)

    for key in keys:
        if key in IGNORED_KEYS.get(cache_name, []):
            continue

        if key in [TO_DATE, FROM_DATE]:
            if is_same_date(elem.get(key), comp_elem.get(key)):
                continue

            if is_technically_none(elem.get(key)) and not is_technically_none(
                comp_elem.get(key)
            ):
                continue

            return False

        if elem[key] != comp_elem[key]:
            return False

    return True


async def compare_elem_to_list(
    elem: dict, compare_list: list, cache_name: CacheNames
) -> bool:
    for comp in compare_list.copy():
        if await compare(elem, comp, cache_name):
            compare_list.remove(comp)
            return True

    return False


async def should_date_be_closed(ref: str, key: str) -> bool:

    ref_date = datetime.datetime.fromisoformat(ref).date()
    today_date = datetime.date.today()

    if key == FROM_DATE:
        if ref_date > today_date:
            return True
        return False

    if key == TO_DATE:
        if ref_date < today_date:
            return True
        return False

    # for mypy
    return False


async def check_for_date_error(elem: dict, ref: dict) -> bool:
    keys = await get_set_of_keys(elem, ref)
    for key in keys:
        if key in [TO_DATE, FROM_DATE]:
            if is_same_date(elem.get(key), ref.get(key)):
                continue
            if is_technically_none(elem.get(key)):
                if await should_date_be_closed(ref.get(key, ""), key):
                    continue
                return False
        if elem.get(key, "") != ref.get(key, ""):
            return False

    return True


async def is_date_error(elem: dict, ref_list: list) -> bool:
    for ref in ref_list:
        if await check_for_date_error(elem, ref):
            return True

    return False


async def compare_by_uuid(
    lora_list: list, gql_list: list, ref_list: list, cache_name: CacheNames
) -> bool:
    for lora in lora_list.copy():
        if await compare_elem_to_list(
            lora, gql_list, cache_name
        ) or await is_date_error(lora, ref_list):
            lora_list.remove(lora)

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
    diff = deepdiff.DeepDiff(lora, gql, verbose_level=2)
    logger.error(diff)
    pprint(diff)


async def compare_single_element(
    lora_cache: dict,
    gql_cache: dict,
    ref_cache: dict,
    cache_name: CacheNames,
    cache_state: str,
) -> bool:
    keys = await get_set_of_keys(lora_cache, gql_cache)

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
        (
            lora_cache.associations,
            gql_cache.associations,
            ref_cache.associations,
            CacheNames.ASSOCIATIONS,
        ),
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
        job = f"{str(name)}_{state}_equivalence_test"
        await notify_prometheus(job, is_equivalent)
        if not is_equivalent:
            is_cache_valid = False

    await notify_prometheus(
        job=f"equiv_test_{state}_lora",
        success=is_cache_valid,
        prometheus_pushgateway=get_gql_cache_settings().prometheus_pushgateway,
    )


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

    await compare_full_caches(
        lora_cache=lora_full_history,
        gql_cache=gql_full_history,
        ref_cache=ref_cache,
        state="Full_History",
    )
    await compare_full_caches(
        lora_cache=lora_skip_past,
        gql_cache=gql_skip_past,
        ref_cache=ref_cache,
        state="Skip_Past",
    )
    await compare_full_caches(
        lora_cache=lora_actual_state,
        gql_cache=gql_actual_state,
        ref_cache=ref_cache,
        state="Actual_State",
    )


@trigger_equiv_router.post("/trigger_cache_equivalence")
async def trigger_cache_equivalence(
    background_tasks: BackgroundTasks,
) -> dict[str, str]:
    background_tasks.add_task(build_caches)
    return {"triggered": "OK"}


@async_to_sync
async def test_cache_equivalence():
    await build_caches()
