from operator import itemgetter
from unittest import TestCase

from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
from more_itertools import unique_everseen

from ..ad_reader import ADParameterReader
from .mocks import MockAD


AD_SAM_ACCOUNT_NAME = "SamAccountName"
AD_CPR_FIELD_NAME = "CprFieldName"
CPR_SEPARATOR = "-"


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
                "cpr_field": AD_CPR_FIELD_NAME,
                "cpr_separator": AD_CPR_FIELD_NAME,
                "properties": [AD_SAM_ACCOUNT_NAME, AD_CPR_FIELD_NAME],
                "sam_filter": "",
                "caseless_samname": True,
            },
        }
        if overridden_settings:
            self.all_settings["primary"].update(overridden_settings)

    def get_from_ad(self, user=None, cpr=None, server=None):
        return self._response


class TestADParameterReader(TestCase):
    """Test `ADParameterReader`"""

    @settings(deadline=None)
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
                "sam_filter": st.text(),
                "caseless_samname": st.text(),
            }
        ),
    )
    def test_uncached_read_user(self, response, settings):
        reader = _TestableADParameterReader(response, **settings)
        ria = []  # accumulates all AD users found
        user = "foobar"
        reader.uncached_read_user(user=user, ria=ria)

        # Test contents of the `ria` accumulator variable. Must contain an AD
        # user for each non-blank SamAccountName *or* CPR. Each AD user must
        # only show up *once* in `ria`.
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

        # TODO: Assert contents of `reader.results`
