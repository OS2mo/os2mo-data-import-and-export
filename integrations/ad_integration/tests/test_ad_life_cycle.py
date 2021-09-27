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
from .mocks import MO_ROOT_ORG_UNIT_UUID
from .mocks import MockADParameterReader
from .mocks import MockEmptyADReader
from .mocks import MockLoraCacheExtended
from .test_utils import TestADWriterMixin


def default_find_primary_engagement(mo_user_uuid):
    """Mock implementation of `MODataSource.find_primary_engagement`"""
    return (
        None,  # = employment_number
        None,  # = title
        MO_ROOT_ORG_UNIT_UUID,  # = eng_org_unit_uuid
        None,  # = eng_uuid
    )


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
        for employee, _ad_object in instance._gen_filtered_employees():
            for engagement in employee["engagements"]:
                # Cast `engagement` to dict to force evaluation of its lazy
                # properties.
                self.assertIsInstance(dict(engagement), dict)

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

    def test_skip_occupied_names_check(self):
        """If `skip_occupied_names_check` is passed, don't call
        `ADParameterReader.cache_all`.
        """
        reader = MockADParameterReader()
        reader.cache_all = mock.Mock()
        self._get_instance(reader=reader, skip_occupied_names_check=True)
        reader.cache_all.assert_not_called()

    def _get_instance(
        self,
        reader=None,
        find_primary_engagement=None,
        users_with_engagements=None,
        create_filters=None,
        disable_filters=None,
        **kwargs,
    ):
        settings = {
            "integrations.ad.write.create_user_trees": [MO_ROOT_ORG_UNIT_UUID],
        }

        if create_filters:
            settings["integrations.ad.lifecycle.create_filters"] = create_filters

        if disable_filters:
            settings["integrations.ad.lifecycle.disable_filters"] = disable_filters

        # Replace `find_primary_engagement` with our mocked version
        self.ad_writer.datasource.find_primary_engagement = (
            find_primary_engagement or default_find_primary_engagement
        )

        load_settings_mock = mock.patch.object(
            ad_life_cycle,
            "load_settings",
            return_value=settings,
        )

        lora_cache_mock = mock.patch.object(
            ad_life_cycle,
            "LoraCache",
            new=lambda **kwargs: MockLoraCacheExtended(self._prepare_static_person()),
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
