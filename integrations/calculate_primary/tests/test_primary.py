import datetime
from unittest import TestCase
from unittest.mock import MagicMock
from collections import OrderedDict
from more_itertools import ilen, unzip
from operator import itemgetter

from hypothesis import given, example
import hypothesis.strategies as st

from ..common import MOPrimaryEngagementUpdater


def engagements_at_date(date, engagements):
    """Filter engagements to only show ones valid at date.

    Args:
        date: The date to check validities against.
        engagements: The list of engagements to filter

    Returns:
        Filtered list of engagements.
    """
    def check_from_date(engagement):
        from_date = datetime.datetime.strptime(
            engagement['validity']['from'], '%Y-%m-%d'
        )
        return from_date <= date

    def check_to_date(engagement):
        if engagement['validity']['to'] is None:
            return True
        to_date = datetime.datetime.strptime(
            engagement['validity']['to'], '%Y-%m-%d'
        )
        return date <= to_date

    return list(filter(check_from_date, filter(check_to_date, engagements)))


class MOPrimaryEngagementUpdaterTest(MOPrimaryEngagementUpdater):
    def __init__(self, *args, **kwargs):
        self.morahelper_mock = MagicMock()
        self.morahelper_mock.read_organisation.return_value = "org_uuid"

        super().__init__(*args, **kwargs)

    def _get_mora_helper(self, mora_base):
        return self.morahelper_mock

    def _find_primary_types(self):
        primary_dict = {
            "fixed_primary": 'fixed_primary_uuid',
            "primary": 'primary_uuid',
            "non_primary": 'non_primary_uuid',
            "special_primary": 'special_primary_uuid',
        }
        primary_list = [
            primary_dict["fixed_primary"],
            primary_dict["primary"],
            primary_dict["special_primary"],
        ]
        return primary_dict, primary_list

    def _calculate_rate_and_ids(self, mo_engagement, no_past):
        raise NotImplementedError()

    def _handle_non_integer_employment_id(self, validity, eng):
        raise NotImplementedError

    def _is_primary(self, employment_id, eng, min_id, impl_specific):
        raise NotImplementedError


class Test_check_user(TestCase):
    """Test the check_user functions."""

    def setUp(self):
        self.updater = MOPrimaryEngagementUpdaterTest({'mora.base': 'mora_base_url'})

    def test_create(self):
        """Test that setUp runs without using it for anything."""
        pass

    @given(st.lists(st.sampled_from([
        'primary_uuid',
        'fixed_primary_uuid',
        'special_primary_uuid',
        'non_primary_uuid',
        'unrelated_uuid'
    ])))
    def test_check_user_non_overlapping(self, engagements):
        """Test the result of running _check_user on non-overlapping engagements.

        Args:
            engagements: A list of engagement_type uuids, these are used to create a
                list of actual engagements, with non-overlapping validities.
        """
        # Create a mapping from 'from date' to engagement.
        # 'from_date' is mocked to be (2930 + list index)
        engagement_map = {
            datetime.datetime(2930 + i, 1, 1): engagement
            for i, engagement in enumerate(engagements)
        }
        # This effectively mocks engagement validities, as:
        #   from: (2930 + list index)
        #   to: (2930 + list index + 1)
        # or in the case of the last engagement until 9999-12-30
        cut_dates = list(engagement_map.keys()) + [
            datetime.datetime(9999, 12, 30, 0, 0)
        ]
        self.updater.morahelper_mock.find_cut_dates.return_value = cut_dates

        # As engagements are made non-overlapping, we will always return only one,
        # namely the one found by lookup in our engagement_map
        self.updater._read_engagement = lambda user_uuid, date: [
            {"engagement_type": {"uuid": engagement_map[date]}}
        ]
        check_filters = [
            # Filter out special primaries
            lambda user_uuid, eng: eng["engagement_type"]["uuid"] != 'special_primary_uuid'
        ]

        def gen_expected(date):
            """Due to non-overlapping 1 or 0 will be returned for either."""
            uuid = engagement_map.get(date)
            count = 1 if uuid in self.updater.primary else 0
            special_count = 1 if uuid == 'special_primary_uuid' else 0
            return count, count - special_count

        self.assertEqual(
            self.updater._check_user(check_filters, 'user_uuid'),
            {date: gen_expected(date) for date in cut_dates[:-1]}
        )

    def engagements_fixture(self):
        """Engagement fixture for testing overlapping engagements."""
        engagements = [{
            'validity': {
                'from': "1931-1-1",
                'to': "1950-1-1",
            },
            'uuid': 'primary_uuid',
        }, {
            'validity': {
                'from': "1939-9-1",
                'to': "1945-9-2",
            },
            'uuid': 'fixed_primary_uuid',
        }, {
            'validity': {
                'from': "1949-1-1",
                'to': None,
            },
            'uuid': 'special_primary_uuid',
        }]
        return engagements

    def test_mora_cut_dates(self):
        """Test that mora cut-dates work as expected."""
        from os2mo_helpers.mora_helpers import MoraHelper
        mora_helper = MoraHelper()
        mora_helper.read_user_engagement = MagicMock()
        mora_helper.read_user_engagement.return_value = self.engagements_fixture()

        cut_dates = mora_helper.find_cut_dates('user_uuid')

        # Expected data derived from engagements_fixture
        self.assertEqual(cut_dates, [
            datetime.datetime(1931, 1, 1),
            datetime.datetime(1939, 9, 1),
            datetime.datetime(1945, 9, 3),  # +1
            datetime.datetime(1949, 1, 1),
            datetime.datetime(1950, 1, 2),  # +1
            datetime.datetime(9999, 12, 30, 0, 0)
        ])

    def test_engagements_at_date(self):
        """Test that engagements_at_date works as expected."""
        engagements = self.engagements_fixture()
        # Expected data derived from engagements_fixture
        engagements_at_date_tests = {
            datetime.datetime(1930, 1, 1): [],
            datetime.datetime(1931, 2, 1): ['primary_uuid'],
            datetime.datetime(1938, 10, 1): ['primary_uuid'],
            datetime.datetime(1939, 10, 1): ['primary_uuid', 'fixed_primary_uuid'],
            datetime.datetime(1945, 8, 1): ['primary_uuid', 'fixed_primary_uuid'],
            datetime.datetime(1946, 8, 1): ['primary_uuid'],
            datetime.datetime(1948, 2, 1): ['primary_uuid'],
            datetime.datetime(1949, 2, 1): ['primary_uuid', 'special_primary_uuid'],
            datetime.datetime(1951, 2, 1): ['special_primary_uuid'],
        }
        for date, expected in engagements_at_date_tests.items():
            filtered_engagements = engagements_at_date(date, engagements)
            engagement_uuids = list(map(itemgetter('uuid'), filtered_engagements))
            self.assertEqual(engagement_uuids, expected)

    def test_check_user_overlapping(self):
        """Test the result of running _check_user on overlapping engagements."""
        # See test_engagement_at_date for details
        engagements = self.engagements_fixture()
        self.updater._read_engagement = lambda user_uuid, date: [
            {"engagement_type": {"uuid": engagement['uuid']}}
            for engagement in engagements_at_date(date, engagements)
        ]

        # See test_mora_cut_dates for details
        cut_dates = [
            datetime.datetime(1931, 1, 1),
            datetime.datetime(1939, 9, 1),
            datetime.datetime(1945, 9, 3),  # +1
            datetime.datetime(1949, 1, 1),
            datetime.datetime(1950, 1, 2),  # +1
            datetime.datetime(9999, 12, 30, 0, 0)
        ]
        self.updater.morahelper_mock.find_cut_dates.return_value = cut_dates

        check_filters = [
            # Filter out special primaries
            lambda user_uuid, eng: eng["engagement_type"]["uuid"] != 'special_primary_uuid'
        ]

        self.assertEqual(
            self.updater._check_user(check_filters, 'user_uuid'),
            {
                # Only primary_uuid
                datetime.datetime(1931, 1, 1, 0, 0): (1, 1),
                # Both primary_uuid and fixed_primary_uuid
                datetime.datetime(1939, 9, 1, 0, 0): (2, 2),
                # Only primary_uuid
                datetime.datetime(1945, 9, 3, 0, 0): (1, 1),
                # Both primary_uuid and special_primary_uuid
                datetime.datetime(1949, 1, 1, 0, 0): (2, 1),
                # Only special_primary_uuid
                datetime.datetime(1950, 1, 2, 0, 0): (1, 0)
            }
        )

    def test_check_user_outputter(self):
        fixture_data = [
            (datetime.datetime(1931, 1, 1, 0, 0), (0, 0)),
            (datetime.datetime(1932, 1, 1, 0, 0), (1, 1)),
            (datetime.datetime(1933, 1, 1, 0, 0), (2, 0)),
            (datetime.datetime(1934, 1, 1, 0, 0), (2, 1)),
            (datetime.datetime(1935, 1, 1, 0, 0), (2, 2)),
        ]
        # It does not normally return an ordered dict, but for testing we want a
        # consistent order.
        self.updater._check_user = lambda check_filters, user_uuid: OrderedDict(
            fixture_data
        )

        outputter, strings, user_uuids, dates = unzip(
            self.updater._check_user_outputter('user_uuid')
        )
        from ..common import noop, logger

        self.assertEqual(list(outputter), [
            print, noop, logger.info, logger.info, print
        ])
        self.assertEqual(list(strings), [
            "No primary", None, "All primaries are special",
            'Only one non-special primary',
            "Too many primaries"
        ])
        self.assertEqual(list(user_uuids), ['user_uuid'] * 5)
        self.assertEqual(list(dates), list(map(itemgetter(0), fixture_data)))
