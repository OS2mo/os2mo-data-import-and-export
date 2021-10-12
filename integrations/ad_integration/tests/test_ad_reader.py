from contextlib import nullcontext
from operator import itemgetter
from unittest import mock
from unittest import TestCase

from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
from more_itertools import unique_everseen

from ..ad_reader import ADParameterReader
from .mocks import MockAD


AD_SAM_ACCOUNT_NAME = "SamAccountName"
AD_CPR_FIELD_NAME = "CprFieldName"


class _TestableADParameterReader(MockAD, ADParameterReader):
    def __init__(self, response, **overridden_settings):
        super().__init__()
        self._response = response
        # Populated by `ADParameterReader.uncached_read_user`
        self.results = {}
        # Read by `ADParameterReader.uncached_read_user` via `AD._get_setting`
        self.all_settings = {
            "global": {"servers": None},
            "primary": {
                "servers": None,
                "search_base": "",
                "properties": [AD_SAM_ACCOUNT_NAME, AD_CPR_FIELD_NAME],
                "cpr_field": AD_CPR_FIELD_NAME,
                "cpr_separator": None,
                "sam_filter": None,
                "caseless_samname": None,
            },
        }
        if overridden_settings:
            self.all_settings["primary"].update(overridden_settings)

    def get_from_ad(self, user=None, cpr=None, server=None):
        return self._response


class TestADParameterReader(TestCase):
    """Test `ADParameterReader`"""

    @settings(max_examples=1000, deadline=None)
    @given(
        # Build simulated AD response
        st.lists(
            st.fixed_dictionaries(
                {
                    AD_SAM_ACCOUNT_NAME: st.text(min_size=1),
                    AD_CPR_FIELD_NAME: st.text() | st.none(),
                }
            )
        ),
        # Build simulated settings
        st.fixed_dictionaries(
            {
                "cpr_separator": st.text(max_size=1),
                "caseless_samname": st.booleans(),
                "sam_filter": st.text(),
            }
        ),
        # Decide whether `first_included` returns an AD user or an empty dict
        st.booleans(),
    )
    def test_uncached_read_user(self, response, settings, first_included_is_empty):
        reader = _TestableADParameterReader(response, **settings)
        ria = []  # accumulates all AD users found
        user = "foobar"

        with self._mock_first_included(first_included_is_empty):
            reader.uncached_read_user(user=user, ria=ria)

        # Test contents of the `ria` accumulator variable
        if first_included_is_empty:
            # `ria` is expected to be empty if `first_included` returns an
            # empty dict.
            expected_ad_users = []
        else:
            # `ria` must contain an AD user for each non-blank SamAccountName
            # *or* CPR. Each AD user must only show up *once* in `ria`.
            expected_ad_users = list(
                unique_everseen(
                    filter(
                        lambda ad_user: (
                            ad_user.get(AD_SAM_ACCOUNT_NAME)
                            or ad_user.get(AD_CPR_FIELD_NAME)
                        ),
                        response,
                    ),
                    key=itemgetter(AD_CPR_FIELD_NAME),
                )
            )
        self.assertListEqual(
            ria,
            expected_ad_users,
            "\nria:\nactual:   %r\nexpected: %r" % (ria, expected_ad_users),
        )

        # TODO: Test contents of `reader.results`
        self.assertTrue(
            len(reader.results) > 0
            if (response and not first_included_is_empty)
            else len(reader.results) == 0
        )

    def _mock_first_included(self, first_included_is_empty):
        if first_included_is_empty:
            path = "integrations.ad_integration.ad_reader.first_included"
            return mock.patch(path, return_value={})
        else:
            return nullcontext()
