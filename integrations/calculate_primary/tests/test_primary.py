import unittest
import datetime
from unittest.mock import MagicMock
from more_itertools import ilen

from hypothesis import given, example
import hypothesis.strategies as st

from ..common import MOPrimaryEngagementUpdater


class MOPrimaryEngagementUpdaterTest(MOPrimaryEngagementUpdater):
    def __init__(self, *args, **kwargs):
        self.morahelper_mock = MagicMock()
        self.morahelper_mock.read_organisation.return_value = "org_uuid"

        super().__init__(*args, **kwargs)

        self.check_filters = [
            # Filter out special primaries
            lambda user_uuid, eng: eng["engagement_type"]["uuid"] != 'special_primary_uuid'
        ]


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


class Test_calculate_primary(unittest.TestCase):
    def setUp(self):
        self.updater = MOPrimaryEngagementUpdaterTest({'mora.base': 'mora_base_url'})

    def test_create(self):
        """Test that setUp runs without using it for anything."""
        pass

    @given(
        engagements=st.lists(
            st.sampled_from([
                'primary_uuid',
                'fixed_primary_uuid',
                'special_primary_uuid',
                'non_primary_uuid',
                'unrelated_uuid'
            ])
        ),
        expected=st.none()
    )
    # No engagements --> No primary
    @example([], (0,0))
    # One engagement --> May be primary
    @example(["primary_uuid"], (1, 1))
    @example(["fixed_primary_uuid"], (1, 1))
    @example(["special_primary_uuid"], (1, 0))
    @example(["non_primary_uuid"], (0, 0))
    @example(["unrelated_uuid"], (0, 0))
    # Multiple engagements --> May have multiple primaries
    @example(["primary_uuid", "primary_uuid"], (2, 2))
    @example(["primary_uuid", "special_primary_uuid"], (2, 1))
    def test_check_user_one_engagement(self, engagements, expected):
        """Test the result of running check_user.

        Args:
            engagements: A list of engagement_type uuids, these are used to create a
                list of actual engagements, with non-overlapping validities.
            expected: An optional 2-tuple return-value, when supplied it is used to
                verify the expected count generated.
        """
        # Create a mapping from 'from date' to engagement.
        # This mocks engagements, such that their validity is:
        #   from: (1930 + list index)
        #   to: (1930 + list index + 1)
        # or in the case of the last engagement until 9999-12-30
        engagement_map = {
            datetime.datetime(1930 + i, 1, 1): engagement
            for i, engagement in enumerate(engagements)
        }
        self.updater.morahelper_mock.find_cut_dates.return_value = list(
            engagement_map.keys()
        ) + [datetime.datetime(9999, 12, 30, 0, 0)]

        # As engagements are made non-overlapping, we will always return only one
        self.updater._read_engagement = lambda user_uuid, date: [
            {"engagement_type": {"uuid": engagement_map[date]}}
        ]

        num_primaries = ilen(filter(
            lambda primary_uuid: primary_uuid in self.updater.primary,
            engagements
        ))
        num_specials = num_primaries - ilen(filter(
            lambda primary_uuid: primary_uuid == 'special_primary_uuid',
            engagements
        ))

        if expected:
            self.assertEqual((num_primaries, num_specials), expected)
        self.assertEqual(
            self.updater._check_user('user_uuid', self.updater.check_filters),
            (num_primaries, num_specials)
        )
