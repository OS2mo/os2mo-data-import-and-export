import time
import typing
from pprint import pprint

import pytest
from deepdiff import DeepDiff
from ra_utils.async_to_sync import async_to_sync
from ra_utils.load_settings import load_settings

from ..gql_lora_cache_async import GQLLoraCache
from ..old_lora_cache import OldLoraCache as LoraCache


# This is an equivalency test, an integration test, and is thus only designed to
# be run manually
class TestEquivalence:
    # The old lora cache had some bugs, this accounts for them when comparing.
    # History and end dates for units, it connections, and managers were bugged, and
    # so was some of the addresses
    def account_for_know_errors(
        self, old_cache: dict, new_cache: dict, cache_type: str
    ) -> dict:

        ref_cache = {}

        lc_gql = GQLLoraCache(resolve_dar=True, full_history=True, skip_past=False)

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

        # for k, mod_list in modified_old_cache.items():
        #     if k not in new_cache or len(mod_list) != len(new_cache[k]):
        #         print("old")
        #         pprint(mod_list)
        #         if k in new_cache:
        #             print("new")
        #             pprint(new_cache[k])

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

    # Compare the caches one by one, run with
    # poetry run pytest -s
    # exporters/sql_export/tests/test_gql_lora_cache.py::TestEquivalence::test_equivalence
    # to see timings as well, omit the '-s' for easier comparison
    @pytest.mark.parametrize("historic", [True, False])
    @pytest.mark.parametrize("skip_past", [True, False])
    # @pytest.mark.parametrize("resolve_dar", [True, False])
    @pytest.mark.parametrize(
        "caches",
        [
            (LoraCache._cache_lora_facets, "facets"),
            (
                LoraCache._cache_lora_classes,
                "classes",
            ),
            (
                LoraCache._cache_lora_itsystems,
                "it_systems",
            ),
            (LoraCache._cache_lora_users, "users"),
            (LoraCache._cache_lora_units, "units"),
            (
                LoraCache._cache_lora_engagements,
                "engagements",
            ),
            (LoraCache._cache_lora_roles, "roles"),
            (LoraCache._cache_lora_leaves, "leaves"),
            (
                LoraCache._cache_lora_it_connections,
                "it_connections",
            ),
            (LoraCache._cache_lora_kles, "kles"),
            (
                LoraCache._cache_lora_related,
                "related",
            ),
            (
                LoraCache._cache_lora_managers,
                "managers",
            ),
            (
                LoraCache._cache_lora_associations,
                "associations",
            ),
            (
                LoraCache._cache_lora_address,
                "addresses",
            ),
        ],
    )
    def test_equivalence(
        self,
        caches: typing.Tuple[typing.Callable, str],
        historic: bool,
        skip_past: bool,
        # resolve_dar: bool
    ) -> None:

        # historic = False
        # skip_past = False
        resolve_dar = True
        old_cache, name = caches

        old_settings = load_settings()

        lc_old = LoraCache(
            full_history=historic,
            skip_past=skip_past,
            resolve_dar=resolve_dar,
            settings=old_settings,
        )

        lc_gql = GQLLoraCache(
            resolve_dar=resolve_dar, full_history=historic, skip_past=skip_past
        )

        if name == "it_connections" or name == "associations":
            lc_old.classes = lc_old._cache_lora_classes()

        if name == "units":
            lc_old.managers = lc_old._cache_lora_managers()

        timestamp0 = time.time()

        oc = old_cache(self=lc_old)

        if name == "units":
            lc_old.units = oc
            lc_old.calculate_derived_unit_data()

        if name == "addresses":
            lc_old.addresses = oc

        timestamp1 = time.time()

        nc = self.new_cache_helper(lc_gql, name)

        timestamp2 = time.time()

        # workaround since the new cache fixes a known problem where the old cache
        # read the wrong end date
        # https://redmine.magenta-aps.dk/issues/53620
        # https://redmine.magenta-aps.dk/issues/53620#note-8
        if name in ("units", "it_connections", "managers", "addresses"):
            oc = self.account_for_know_errors(
                old_cache=oc, new_cache=nc, cache_type=name
            )

        print(
            f"timings {name}: historic: {historic}, skip_past: {skip_past} "
            f"new: {timestamp1 - timestamp0}, old: {timestamp2 - timestamp1} "
            # f" new: {timestamp2 - timestamp1} "
            f"slowdown: {(timestamp1 - timestamp0) / (timestamp2 - timestamp1)} "
        )

        assert oc == nc

    # for debugging a single cache at a time
    def test_equivalence_single(self) -> None:
        historic: bool = True
        skip_past: bool = False
        resolve_dar = True
        name = "single run"

        old_settings = load_settings()

        lc_old = LoraCache(
            full_history=historic,
            skip_past=skip_past,
            resolve_dar=resolve_dar,
            settings=old_settings,
        )

        lc_gql = GQLLoraCache(
            resolve_dar=resolve_dar, full_history=historic, skip_past=skip_past
        )

        pprint(lc_gql._get_org_uuid())

        timestamp0 = time.time()
        # hvis det fejler fordi der ikke er cachet klasser, så indkommenter det her
        # lc_old.cache_classes()

        old_cache = lc_old._cache_lora_address()
        # lc_old.addresses = old_cache
        # dar = lc_old._cache_dar()
        # pprint(lc_old.dar_map)
        timestamp1 = time.time()

        new_cache = self.new_cache_helper(lc_gql, "name")

        timestamp2 = time.time()

        old_cache = self.account_for_know_errors(old_cache, new_cache, "addresses")

        # pprint(old_cache)
        # pprint(new_cache)

        print(
            f"timings {name}: historic: {historic}, skip_past: {skip_past} "
            f"old: {timestamp1 - timestamp0}, new: {timestamp2 - timestamp1} "
            f"slowdown: {(timestamp2 - timestamp1) / (timestamp1 - timestamp0)} "
        )

        # problem = "38d1366a-407c-4768-af4d-ceee558fcd31"
        # problem = "1ac9f6ba-0dcc-4c87-8ab1-5b88eae42d46"
        # problem = "7c5d4722-9a89-4d45-93e1-522a84e6303a"
        # problem = "1a3241f1-e230-4ccb-970c-b2b89b08cbe7"
        # problem = "a54127f4-74f6-4931-ab2d-ce5a0ac4b610"
        # print("old")
        # pprint(old_cache[problem])
        # print("new")
        # pprint(new_cache[problem])
        # pprint(dar["0a3f50bc-5bfb-32b8-e044-0003ba298018"])
        # pprint(dar)
        # Det her tager mere eller mindre højde for forkerte datoer. Der mangler lidt
        # arbejde med at tage højde for gamle elementer
        # old_cache = self.account_for_know_errors(old_cache, new_cache)

        # pprint(old_cache[problem])
        # pprint(new_cache[problem])

        # for k, v in old_cache.items():
        #     for v2 in v:
        #         if v2['primary_boolean'] is not None:
        #             pprint(v)

        # for k, v in new_cache.items():
        #     if v[0]['to_date'] is not None:
        #         date = dateutil.parser.parse(v[0]['to_date']).date()
        #         if date < datetime.datetime.now().date():
        #             pprint(v)

        assert old_cache == new_cache

    # Test populate, making a complete task. Also used for performance testing
    # poetry run pytest -s
    # exporters/sql_export/tests/test_gql_lora_cache.py::TestEquivalence::test_equivalence
    # omit the '-s' for only test results, keep it for timings
    @pytest.mark.parametrize("historic", [True, False])
    @pytest.mark.parametrize("skip_past", [True, False])
    # @pytest.mark.parametrize("resolve_dar", [True, False])
    def test_populate(
        self,
        historic: bool,
        skip_past: bool,
        # resolve_dar: bool
    ):
        def comp_caches(oldc, newc):
            pprint(DeepDiff(oldc, newc, verbose_level=2))

        # historic = False
        # skip_past = False
        resolve_dar = True

        name = "populate test"

        old_cache = LoraCache(
            resolve_dar=resolve_dar, full_history=historic, skip_past=skip_past
        )

        new_cache = GQLLoraCache(
            resolve_dar=resolve_dar, full_history=historic, skip_past=skip_past
        )

        new_cache.settings.start_logging_based_on_settings()

        timestamp0 = time.time()

        new_cache.populate_cache(dry_run=False, skip_associations=False)

        timestamp1 = time.time()

        print(f" new: {timestamp1 - timestamp0}", flush=True)

        old_cache.populate_cache(dry_run=False, skip_associations=False)
        old_cache.calculate_derived_unit_data()

        timestamp2 = time.time()

        print(
            f"timings {name}: historic: {historic}, skip_past: {skip_past} "
            f"new: {timestamp1 - timestamp0}, old: {timestamp2 - timestamp1} "
            # f" new: {timestamp1 - timestamp0} "
            f"slowdown: {(timestamp1 - timestamp0) / (timestamp2 - timestamp1)} "
        )

        comp_caches(old_cache.facets, new_cache.facets)
        comp_caches(old_cache.classes, new_cache.classes)
        comp_caches(old_cache.itsystems, new_cache.itsystems)
        comp_caches(old_cache.users, new_cache.users)
        comp_caches(
            self.account_for_know_errors(old_cache.units, new_cache.units, "units"),
            new_cache.units,
        )
        comp_caches(old_cache.engagements, new_cache.engagements)
        comp_caches(old_cache.roles, new_cache.roles)
        comp_caches(old_cache.leaves, new_cache.leaves)
        comp_caches(
            self.account_for_know_errors(
                old_cache.it_connections,
                new_cache.it_connections,
                "it_connections",
            ),
            new_cache.it_connections,
        )
        comp_caches(old_cache.kles, new_cache.kles)
        comp_caches(old_cache.related, new_cache.related)
        comp_caches(
            self.account_for_know_errors(
                old_cache.managers,
                new_cache.managers,
                "managers",
            ),
            new_cache.managers,
        )
        comp_caches(old_cache.associations, new_cache.associations)
        comp_caches(
            self.account_for_know_errors(
                old_cache.addresses, new_cache.addresses, "addresses"
            ),
            new_cache.addresses,
        )
