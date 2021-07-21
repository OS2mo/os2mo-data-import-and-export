from unittest import mock
from unittest import TestCase

from more_itertools import first_true

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

    def setUp(self):
        super().setUp()
        self._setup_adwriter()

    def test_create_ad_accounts_empty_ad(self):
        """An AD user must be created when the AD is empty.

        In this test, we mock an empty AD. Therefore we expect exactly one AD
        write (`New-ADUser`) to be performed.
        """
        instance = self._get_instance()
        stats = instance.create_ad_accounts()
        self._assert_stats_equal(
            stats,
            created_users=1,
            users={self._prepare_static_person()["uuid"]},
        )
        # Assert the proper AD writes were issued: 2 reads (?) and 1 write.
        self._assert_ad_write(num_scripts=3, expected_script_content="New-ADUser")

    def test_create_ad_accounts_empty_ad_and_no_primary_engagement(self):
        """An AD user must *not* be created when the MO user has no primary
        engagement.

        In this test, the AD is empty, but the MO user has no primary
        engagements. Consequently, no AD users are created.
        """
        instance = self._get_instance(
            find_primary_engagement=mock.Mock(
                side_effect=NoPrimaryEngagementException()
            ),
        )
        stats = instance.create_ad_accounts()
        self._assert_stats_equal(stats, not_in_user_tree=1)
        self._assert_no_ad_writes()

    def test_create_ad_accounts_filter_user_without_engagements(self):
        """An AD user must *not* be created when `users_with_engagements` is
        empty.
        """
        instance = self._get_instance(users_with_engagements={})
        stats = instance.create_ad_accounts()
        self._assert_stats_equal(stats, no_active_engagements=1)
        self._assert_no_ad_writes()

    def test_create_ad_accounts_user_already_in_ad(self):
        """An AD user must *not* be created if the AD user already exists.

        In this test, the MO user has already been "created" in AD, as the
        `MockADReader` always returns the same "person" regardless of the CPR
        number looked up.
        """
        instance = self._get_instance(reader=MockADParameterReader())
        stats = instance.create_ad_accounts()
        self._assert_stats_equal(stats, already_in_ad=1)
        self._assert_no_ad_writes()

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

    def _get_instance(
        self,
        reader=None,
        find_primary_engagement=None,
        users_with_engagements=None,
    ):
        settings = {
            "integrations.ad.write.create_user_trees": [MO_ROOT_ORG_UNIT_UUID],
        }

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
                        instance = ad_life_cycle.AdLifeCycle()
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
