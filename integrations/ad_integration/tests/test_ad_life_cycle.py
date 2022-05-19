import copy
from typing import Callable
from typing import Dict
from typing import Optional
from unittest import mock
from unittest import TestCase
from uuid import uuid4

from more_itertools import first_true
from parameterized import parameterized

from .. import ad_life_cycle
from ..ad_exceptions import NoPrimaryEngagementException
from .mocks import MO_CHILD_ORG_UNIT_UUID
from .mocks import MO_ROOT_ORG_UNIT_UUID
from .mocks import MockADParameterReader
from .mocks import MockEmptyADReader
from .mocks import MockLoraCacheDanglingParentUnit
from .mocks import MockLoraCacheEmptyEmployee
from .mocks import MockLoraCacheEmptyUnit
from .mocks import MockLoraCacheExtended
from .mocks import MockLoraCacheParentChildUnit
from .mocks import MockLoraCacheParentUnitUnset
from .test_utils import TestADWriterMixin


BASE_SETTINGS = {"integrations.ad.write.create_user_trees": [MO_ROOT_ORG_UNIT_UUID]}


def mock_find_primary_engagement(eng_org_unit_uuid):
    """Return mock implementation of `MODataSource.find_primary_engagement`"""

    def mock(mo_user_uuid):
        return (
            None,  # = employment_number
            None,  # = title
            eng_org_unit_uuid,
            None,  # = eng_uuid
        )

    return mock


class TestAdLifeCycle(TestCase, TestADWriterMixin):
    """Test `ad_life_cycle`"""

    maxDiff = None

    def setUp(self):
        super().setUp()
        self._setup_adwriter()

    @parameterized.expand(
        [
            # 1. An AD user must be created when the AD is empty.
            # In this test, we mock an empty AD. Therefore we expect exactly
            # one AD write (`New-ADUser`) to be performed.
            (
                # Empty `_get_instance` kwargs
                {},
                # Expected stats
                lambda instance: {
                    "created_users": 1,
                    "users": {instance._prepare_static_person()["uuid"]},
                },
                # Assert the proper AD writes were issued: 2 reads (?) and 1
                # write
                {"num_scripts": 3, "expected_script_content": "New-ADUser"},
            ),
            # 2. An AD user must *not* be created when the MO user has no
            # primary engagement.
            # In this test, the AD is empty, but the MO user has no primary
            # engagements. Consequently, no AD users are created.
            (
                # Mock a MO user without a primary engagement
                {
                    "find_primary_engagement": mock.Mock(
                        side_effect=NoPrimaryEngagementException()
                    ),
                },
                # Expected stats
                lambda instance: {
                    "not_in_user_tree": 1,
                    "skipped": instance._get_expected_skipped_entry(
                        "filter_users_outside_unit_tree"
                    ),
                },
                # Assert no AD writes were done
                None,
            ),
            # 3. An AD user must *not* be created when `users_with_engagements`
            # is empty.
            (
                # Mock a MO user without *any* engagements
                {"users_with_engagements": {}},
                # Expected stats
                lambda instance: {
                    "no_active_engagements": 1,
                    "skipped": instance._get_expected_skipped_entry(
                        "filter_user_without_engagements"
                    ),
                },
                # Assert no AD writes were done
                None,
            ),
            # 4. An AD user must *not* be created if the AD user already
            # exists.
            (
                # In this test, the MO user has already been "created" in AD,
                # as the `MockADReader` always returns the same "person"
                # regardless of the CPR number looked up.
                {"reader": MockADParameterReader()},
                # Expected stats
                lambda instance: {
                    "already_in_ad": 1,
                    "skipped": instance._get_expected_skipped_entry(
                        "filter_user_already_in_ad"
                    ),
                },
                # Assert no AD writes were done
                None,
            ),
            # 5. An AD user must *not* be created if at least one "create
            # filter" returns `True` for the corresponding MO user.
            (
                # A "create filter" is a Jinja template evaluating to `True` or
                # `False`. Here is a "create filter" which never returns `True`
                # for our test MO user.
                {"create_filters": ["{{ employee['employment_number'] == '1st' }}"]},
                # Expected stats
                lambda instance: {
                    "create_filtered": 1,
                    "skipped": instance._get_expected_skipped_entry(
                        "create_filters_num_0"
                    ),
                },
                # Assert no AD writes were done
                None,
            ),
        ]
    )
    def test_create_ad_accounts(
        self,
        instance_kwargs: Dict,
        expected_stats_func: Callable,
        expected_ad_writes: Optional[Dict],
    ):
        instance = self._get_instance(**instance_kwargs)
        stats = instance.create_ad_accounts()
        self._assert_stats_equal(stats, **expected_stats_func(self))
        if expected_ad_writes:
            self._assert_ad_write(**expected_ad_writes)
        else:
            self._assert_no_ad_writes()

    @parameterized.expand(
        [
            # 1. If the AD user exists, and the MO user has *no* engagements,
            # the AD user should be disabled.
            (
                {
                    # Pretend AD user already exists
                    "reader": MockADParameterReader(),
                    # Pretend the MO user has no engagements
                    "users_with_engagements": {},
                },
                # Expected stats
                lambda instance: {
                    "disabled_users": 1,
                    "users": {instance._prepare_static_person()["uuid"]},
                },
                # Assert one AD write is done, which disables the AD user
                {"num_scripts": 1, "expected_script_content": "Disable-ADAccount"},
            ),
            # 2. If the AD user exists, and the MO user has *no* engagements,
            # *but* a "disable filter" filters out the MO user, the AD user
            # should *not* be disabled.
            (
                {
                    # Pretend AD user already exists
                    "reader": MockADParameterReader(),
                    # Pretend the MO user has no engagements
                    "users_with_engagements": {},
                    # Add a "disable filter" which evaluates to `False`, which
                    # means this user should be skipped in
                    # `AdLifeCycle.disable_ad_accounts`.
                    "disable_filters": ["{{ employee['employment_number'] == '1st' }}"],
                },
                # Expected stats
                lambda instance: {
                    "skipped": instance._get_expected_skipped_entry(
                        "disable_filters_num_0"
                    ),
                },
                # Assert no AD writes were done
                None,
            ),
            # 3. If the AD user exists, but the MO user *has* engagements, the
            # AD user should *not* be disabled.
            (
                {
                    # Pretend AD user already exists
                    "reader": MockADParameterReader(),
                    # Pretend the MO user *has* engagements
                    "users_with_engagements": None,
                },
                # Expected stats
                lambda instance: {"disabled_users": 0, "users": set()},
                # Assert no AD writes were done
                None,
            ),
            # 4. If the AD user does not exist, and the MO does not have any
            # engagements, the AD user should *not* be disabled.
            (
                {
                    # Pretend AD does not exist
                    "reader": None,
                    # Pretend the MO user has no engagements
                    "users_with_engagements": {},
                },
                # Expected stats
                lambda instance: {"disabled_users": 0, "users": set()},
                # Assert no AD writes were done
                None,
            ),
        ]
    )
    def test_disable_ad_accounts(
        self,
        instance_kwargs: Dict,
        expected_stats_func: Callable,
        expected_ad_writes: Optional[Dict],
    ):
        """An AD user must *not* be disabled if present in
        `users_with_engagements`.
        An AD user can only be disabled if already present in the AD.
        """
        instance = self._get_instance(**instance_kwargs)
        stats = instance.disable_ad_accounts()
        self._assert_stats_equal(stats, **expected_stats_func(self))
        if expected_ad_writes:
            self._assert_ad_write(**expected_ad_writes)
        else:
            self._assert_no_ad_writes()

    def test_disable_ad_accounts_handles_status(self):
        """If disabling an AD user fails, the status should be recorded."""
        # Get an `AdLifeCycle` instance which is set up to disable one AD user
        instance = self._get_instance(
            reader=MockADParameterReader(), users_with_engagements={}
        )
        # But replace `ADWriter.enable_user` with a function indicating a
        # failed AD update.
        instance.ad_writer.enable_user = lambda **kwargs: (False, "message")
        stats = instance.disable_ad_accounts()
        self._assert_stats_equal(stats, disabled_users=0, critical_errors=1)

    def test_gen_filtered_employees(self):
        """The lazy dicts produced by `_gen_filtered_employees` must be able
        to be evaluated.
        """
        # Consume the lazy dicts produced by `_gen_filtered_employees` to
        # verify their contents.
        instance = self._get_instance()
        consumed = list(instance._gen_filtered_employees())
        self.assertNotEqual(consumed, [])
        for employee, _ad_object in consumed:
            for engagement in employee["engagements"]:
                # Cast `engagement` to dict to force evaluation of its lazy
                # properties.
                self.assertIsInstance(dict(engagement), dict)

    def test_gen_filtered_employees_handles_empty_employees(self):
        instance = self._get_instance(mock_lora_cache_class=MockLoraCacheEmptyEmployee)
        consumed = list(instance._gen_filtered_employees())
        self.assertListEqual(consumed, [])

    @parameterized.expand(
        [
            # 1. Mock employee data from MO API
            (
                # Mock employee data
                dict(uuid=uuid4(), name=("Givenname Middlename", "Surname")),
                # Expected name in log
                "Givenname Middlename Surname",
            ),
            # 2. Mock employee data from LoraCache
            (
                # Mock employee data
                dict(uuid=uuid4(), navn="Givenname Middlename Surname"),
                # Expected name in log
                "Givenname Middlename Surname",
            ),
            # 3. Mock bogus employee data without "name" or "navn" keys
            (
                # Mock employee data
                dict(uuid=uuid4()),
                # Expected name in log
                "unknown",
            ),
        ]
    )
    def test_log_skipped_decorator(self, employee, expected_name):
        instance = self._get_instance()

        @instance.log_skipped("foobar")
        def foobar(tup):
            return False

        # Call decorated function with mock `(employee, ad_user)` tuple
        foobar((employee, None))

        # Expect to find a dictionary mapping employee UUID to employee name
        # under `instance.stats["skipped"]["foobar"]`.
        self.assertDictEqual(
            instance.stats["skipped"]["foobar"],
            {employee["uuid"]: expected_name},
        )

    @parameterized.expand(
        [
            # Case 1: we look for the root unit and find it.
            (
                mock_find_primary_engagement(MO_ROOT_ORG_UNIT_UUID),
                MockLoraCacheExtended,
                True,
            ),
            # Case 2: we look for a child of the root unit, and find it.
            (
                mock_find_primary_engagement(MO_CHILD_ORG_UNIT_UUID),
                MockLoraCacheParentChildUnit,
                True,
            ),
            # Case 3: we look for the root unit, but there are no units at all.
            (
                mock_find_primary_engagement(MO_ROOT_ORG_UNIT_UUID),
                MockLoraCacheEmptyUnit,
                False,
            ),
            # Case 4: we look for a child unit, but the parent-child relation
            # is 'broken', and we can not follow the relationship up to the
            # root node.
            (
                mock_find_primary_engagement(MO_CHILD_ORG_UNIT_UUID),
                MockLoraCacheDanglingParentUnit,
                False,
            ),
            # Case 5: we look for a child unit, but the child node has an unset
            # parent node reference, and we cannot find the root unit.
            (
                mock_find_primary_engagement(MO_CHILD_ORG_UNIT_UUID),
                MockLoraCacheParentUnitUnset,
                False,
            ),
        ]
    )
    def test_find_user_unit_tree(
        self,
        find_primary_engagement: Callable,
        mock_lora_cache_class: Callable,
        expected_result: bool,
    ):
        instance = self._get_instance(
            find_primary_engagement=find_primary_engagement,
            mock_lora_cache_class=mock_lora_cache_class,
        )
        result = instance._find_user_unit_tree(({"uuid": uuid4()}, {}))
        self.assertEqual(result, expected_result)

    def test_preview_command_for_uuid(self):
        with mock.patch("click.echo_via_pager") as mock_echo:
            instance = self._get_instance()
            commands = ad_life_cycle.run_preview_command_for_uuid(instance, "mo_uuid")
            self.assertEqual(mock_echo.call_count, 2)
            self.assertEqual(len(commands), 2)
            self.assertIsInstance(commands[0], str)
            self.assertIsInstance(commands[1], str)

    def test_unhandled_exception(self, *args):
        instance = self._get_instance()
        with mock.patch.object(self.ad_writer, "create_user", side_effect=Exception):
            with self.assertLogs("export") as cm:
                instance.create_ad_accounts()
                self.assertEqual(len(cm.records), 1)
                self.assertRegex(
                    cm.records[0].message,
                    r"Error creating AD user for MO user '.*?': .*",
                )

    def _get_instance(
        self,
        reader=None,
        find_primary_engagement=None,
        users_with_engagements=None,
        create_filters=None,
        disable_filters=None,
        mock_lora_cache_class=MockLoraCacheExtended,
        **kwargs,
    ):
        settings = copy.copy(BASE_SETTINGS)

        if create_filters:
            settings["integrations.ad.lifecycle.create_filters"] = create_filters

        if disable_filters:
            settings["integrations.ad.lifecycle.disable_filters"] = disable_filters

        # Replace `find_primary_engagement` with our mocked version
        self.ad_writer.datasource.find_primary_engagement = (
            find_primary_engagement
            or mock_find_primary_engagement(MO_ROOT_ORG_UNIT_UUID)
        )

        load_settings_mock = mock.patch.object(
            ad_life_cycle,
            "load_settings",
            return_value=settings,
        )
        read_settings_mock = mock.patch.object(
            ad_life_cycle,
            "injected_settings",
            return_value=settings,
        )

        lora_cache_mock = mock.patch.object(
            ad_life_cycle,
            "LoraCache",
            new=lambda **kwargs: mock_lora_cache_class(self._prepare_static_person()),
        )

        reader_mock = mock.patch.object(
            ad_life_cycle,
            "ADParameterReader",
            new=lambda: reader or MockEmptyADReader(),
        )

        writer_mock = mock.patch.object(
            ad_life_cycle,
            "ADWriter",
            new=lambda **kwargs: self.ad_writer,
        )

        with load_settings_mock:
            with read_settings_mock:
                with lora_cache_mock:
                    with reader_mock:
                        with writer_mock:
                            instance = ad_life_cycle.AdLifeCycle(**kwargs)
                            # Replace `users_with_engagements` dict attr with our
                            # mocked version.
                            if users_with_engagements is not None:
                                instance.users_with_engagements = {}
                            return instance

    def _assert_stats_equal(self, actual_stats, **expected_stats):
        _keys = {
            "critical_errors",
            "engagement_not_found",
            "created_users",
            "disabled_users",
            "already_in_ad",
            "no_active_engagements",
            "not_in_user_tree",
            "create_filtered",
        }
        _expected_stats = dict.fromkeys(_keys, 0)
        _expected_stats["users"] = set()
        _expected_stats.update(expected_stats)
        self.assertDictEqual(actual_stats, _expected_stats)

    def _assert_no_ad_writes(self):
        self.assertEqual(len(self.ad_writer.scripts), 0)

    def _assert_ad_write(self, num_scripts=None, expected_script_content=None):
        self.assertEqual(len(self.ad_writer.scripts), num_scripts)
        first_true(
            self.ad_writer.scripts,
            pred=lambda script: expected_script_content in script,
        )

    def _get_expected_skipped_entry(self, filtername):
        mo_user = self._prepare_static_person()
        return {filtername: {mo_user["uuid"]: " ".join(mo_user["name"])}}


class _TestableAdLifeCycle(ad_life_cycle.AdLifeCycle):
    def _load_settings(self):
        return BASE_SETTINGS

    def _get_adreader(self):
        return mock.MagicMock()

    def _get_adwriter(self, **kwargs):
        return mock.MagicMock(**kwargs)

    def _update_lora_cache(self, dry_run: bool = True):
        return mock.MagicMock(), mock.MagicMock()


class TestOccupiedNamesCheckFlag(TestCase):
    @parameterized.expand(
        [
            (True,),
            (False,),
        ]
    )
    @mock.patch.object(
        ad_life_cycle,
        "injected_settings",
    )
    def test_skip_occupied_names_check(self, value, injected_settings_mock):
        """If `skip_occupied_names_check` is passed, pass it to `ADWriter`"""
        instance = _TestableAdLifeCycle(skip_occupied_names_check=value)
        self.assertEqual(instance.ad_writer.skip_occupied_names, value)
