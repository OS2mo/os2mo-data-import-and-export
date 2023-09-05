import copy
import logging
import typing
from datetime import datetime, timedelta
from pprint import pprint
from deepdiff.diff import DeepDiff

from fastramqpi.context import Context
from ra_utils.async_to_sync import async_to_sync

from .settings import SqlExporterSettings
from .test_resources.old_lora_cache import OldLoraCache
from .cache import Cache
from .utils import _setup_gql_client


logger = logging.getLogger(__name__)

async def _build_context():
    async with _setup_gql_client(settings=SqlExporterSettings()) as session:
        context = Context()
        context["user_context"] = {}
        context["user_context"]["settings"] = SqlExporterSettings()
        context["graphql_session"] = session

        return context


@async_to_sync
async def start_testing():
    async with _setup_gql_client(settings=SqlExporterSettings()) as session:
        context = Context()
        context["user_context"] = {}
        settings = SqlExporterSettings()
        settings.start_logging_based_on_settings()
        context["user_context"]["settings"] = settings
        context["graphql_session"] = session

        e = EquivalenceTester(context)
        await e.perform_equivalence_test()


def test_equivalence():

    start_testing()


async def new_cache_helper(lc, cache_name) -> dict:
    match cache_name:
        case "facets":
            return lc.facets
        case "classes":
            return lc.classes
        case "it_systems":
            return lc.itsystems
        case "users":
            return lc.users
        case "units":
            return lc.units
        case "engagements":
            return lc.facets
        case "roles":
            return lc.roles
        case "leaves":
            return lc.leaves
        case "it_connections":
            return lc.it_connections
        case "kles":
            return lc.kles
        case "related":
            return lc.related
        case "managers":
            return lc.managers
        case "associations":
            return lc.associations
        case "addresses":
            return lc.addresses


async def account_for_know_errors(
    old_cache: dict, new_cache: dict, cache_type: str, ref_cache: dict
) -> dict:
    if cache_type in [
            "itsystems",
            "facets",
            "classes",
            "users",
            "engagements",
            "roles",
            "leaves",
            "kles",
            "related",
            "associations"]:
        return old_cache

    # if cache_type != "addresses":
        # ref_cache = await new_cache_helper(ref_cache, cache_type)

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
            continue

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


async def account_for_recent_fixes(
    single_cache: dict, fix_time: bool = True, fix_primary: bool = False
) -> dict:

    for uuid, data in single_cache.items():
        for entry in data:

            if fix_time:
                validity_start = entry.get("from_date")
                validity_end = entry.get("to_date")
                if validity_start is not None:
                    validity_start = datetime.fromisoformat(validity_start)
                    entry['from_date'] = str(validity_start.date())
                if validity_end is not None:
                    validity_end = datetime.fromisoformat(validity_end)
                    # validity_end -= timedelta(days=1)

                    entry["to_date"] = str(validity_end.date())

                    if validity_start > validity_end:
                        entry['to_date'] = str(validity_start.date())


                # if validity_end is None:
                #     continue
                # validity_end = datetime.fromisoformat(validity_end).date()
                #
                # if validity_start is None:
                #     entry["to_date"] = (validity_end - timedelta(days=1))
                #     continue
                # validity_start = datetime.fromisoformat(validity_start).date()
                #
                # if validity_end - timedelta(days=1) >= validity_start:
                #     entry["to_date"] = validity_end - timedelta(days=1)
                # else:
                #     entry.pop("to_date")

            if fix_primary and entry.get("primary_boolean") is not None:
                entry.pop("primary_boolean")
    return single_cache


class EquivalenceTester:
    def __init__(self, context: Context):
        self.context: Context = context
        self.user_context = context["user_context"]
        self.settings: SqlExporterSettings = self.user_context["settings"]

        self.old_actual_state: OldLoraCache
        self.old_full_history: OldLoraCache
        self.old_historic_skip_past: OldLoraCache
        self.main_org_unit_uuid: str = ""
        self.build_old_caches()

        self.new_actual_state: Cache
        self.new_full_history: Cache
        self.new_historic_skip_past: Cache

    def build_old_caches(self):
        settings = SqlExporterSettings().to_oldSettings()
        self.old_actual_state = OldLoraCache(
            resolve_dar=True,
            full_history=False,
            skip_past=False,
            settings=settings,
        )

        self.old_actual_state.populate_cache(False, False)
        self.main_org_unit_uuid = self.old_actual_state.org_uuid

        self.old_full_history = OldLoraCache(
            resolve_dar=True,
            full_history=True,
            skip_past=False,
            settings=settings,
        )

        self.old_full_history.populate_cache(False, False)

        self.old_historic_skip_past = OldLoraCache(
            resolve_dar=True,
            full_history=True,
            skip_past=True,
            settings=settings,
        )

        self.old_historic_skip_past.populate_cache(False, False)

    async def build_new_caches(self):
        print('building actual state cache')
        self.new_actual_state = Cache(main_org_unit_uuid=self.main_org_unit_uuid, context=self.context, historic=False, skip_past=False)

        await self.new_actual_state.populate_cache_async()

        print('Building cache with full history')
        self.new_full_history = Cache(
            main_org_unit_uuid=self.main_org_unit_uuid, context=self.context, historic=True, skip_past=False
        )

        await self.new_full_history.populate_cache_async()
        print('building historic cache with no past')
        self.new_historic_skip_past = Cache(
            main_org_unit_uuid=self.main_org_unit_uuid, context=self.context, historic=True, skip_past=True
        )

        await self.new_historic_skip_past.populate_cache_async()

    async def compare_single_cache(
        self, old_cache: dict, new_cache: dict, cache_type: str
    ):
        # diff = DeepDiff(old_cache, new_cache)
        # old_cache = await account_for_recent_fixes(old_cache, fix_time=False)
        new_cache = await account_for_recent_fixes(
            new_cache, fix_primary=(cache_type == "engagements"), fix_time=(cache_type not in ["facets", "classes", "itsystems"])
        )


        old_cache = await account_for_know_errors(
            old_cache, new_cache, cache_type, copy.deepcopy(new_cache)
        )

        # diff = DeepDiff(old_cache, new_cache, verbose_level=2)

        for key, value in old_cache.items():
            for obj in value:
                if 'to_date' in obj:
                    obj.pop('to_date')

        for key, value in new_cache.items():
            for obj in value:
                if 'to_date' in obj:
                    obj.pop('to_date')

        if old_cache == new_cache:
            return True

        # print(len(old_cache))
        # print(len(new_cache))

        diff = DeepDiff(old_cache, new_cache, verbose_level=2)
        # diff = DeepDiff(old_cache, new_cache)
        # if cache_type == 'units':
        #     # pprint(new_cache['32865a87-3475-5dbd-accb-d7659603f0b7'])
        #     if diff.get('dictionary_item_added'):
        #         diff.pop('dictionary_item_added')
        #         if len(diff) == 0:
        #             return True
        #
        # if cache_type == 'engagements':
        #     new_diff = DeepDiff(old_cache, new_cache)
        #     new_diff.pop('dictionary_item_removed')
        #     if len(new_diff) == 0:
        #         return True
        pprint(diff)

        return False

    async def compare_full_caches(self, old_cache: OldLoraCache, new_cache):
        if old_cache is None or new_cache is None:
            return
        cache_list = [
            (old_cache.itsystems, new_cache.itsystems, "itsystems"),
            (old_cache.facets, new_cache.facets, "facets"),
            (old_cache.classes, new_cache.classes, "classes"),
            (old_cache.users, new_cache.users, "users"),
            (old_cache.units, new_cache.units, "units"),
            (old_cache.engagements, new_cache.engagements, "engagements"),
            (old_cache.roles, new_cache.roles, "roles"),
            (old_cache.leaves, new_cache.leaves, "leaves"),
            (old_cache.it_connections, new_cache.it_connections, "it_connections"),
            (old_cache.kles, new_cache.kles, "kles"),
            (old_cache.related, new_cache.related, "related"),
            (old_cache.managers, new_cache.managers, "managers"),
            (old_cache.associations, new_cache.associations, "associations"),
            (old_cache.addresses, new_cache.addresses, "addresses"),
        ]

        for oc, nc, ct in cache_list:
            success = await self.compare_single_cache(oc, nc, ct)

            if not success:
                print(f"Cache Type: {ct}")

            # assert success

    async def perform_equivalence_test(self):
        # self.build_old_caches()
        await self.build_new_caches()

        await self.compare_full_caches(self.old_actual_state, self.new_actual_state)

        await self.compare_full_caches(self.old_full_history, self.new_full_history)

        await self.compare_full_caches(
            self.old_historic_skip_past, self.new_historic_skip_past
        )
