from unittest.mock import Mock

from os2mo_helpers.mora_helpers import MoraHelper
from winrm import Session

from ..ad_common import AD
from ..ad_writer import MORESTSource
from .test_utils import TestADWriterMixin

MO_ROOT_ORG_UNIT_UUID = "not-a-mo-org-unit-uuid"
MO_UUID = "not-a-uuid"
AD_UUID_FIELD = "uuidField"
UNKNOWN_CPR_NO = "not-a-cpr-no"


class MockAD(AD):
    def __init__(self):
        self.session = Mock(spec=Session)
        self.all_settings = {"primary": {"search_base": ""}}


class MockADParameterReader(TestADWriterMixin):
    """Mock implementation of `ADParameterReader` which always returns the same
    AD user."""

    def read_user(self, cpr=None, **kwargs):
        return self._prepare_get_from_ad(ad_transformer=None)

    def read_it_all(self, **kwargs):
        return [self.read_user()]

    def cache_all(self, **kwargs):
        return self.read_it_all()


class MockEmptyADReader(MockADParameterReader):
    """Mock implementation of `ADParameterReader` which simulates an empty AD"""

    def read_user(self, **kwargs):
        return None

    def read_it_all(self, **kwargs):
        return []

    def cache_all(self, **kwargs):
        return self.read_it_all()


class MockMORESTSource(MORESTSource):
    def __init__(self, from_date, to_date):
        self.from_date = from_date
        self.to_date = to_date

    def get_engagement_dates(self, uuid):
        # Return 2-tuple of (from_dates, to_dates)
        return [self.from_date], [self.to_date]


class MockLoraCache:
    # This implements enough of the real `LoraCache` to make
    # `ad_sync.AdMoSync._edit_engagement` happy.

    def __init__(self, mo_values, mo_engagements=None):
        self._mo_values = mo_values
        self._mo_engagements = mo_engagements

    @property
    def users(self):
        return {self._mo_values["uuid"]: [self._mo_values]}

    @property
    def engagements(self):
        extensions = {"udvidelse_%d" % n: "old mo value #%d" % n for n in range(1, 11)}
        if self._mo_engagements:
            return {
                eng["uuid"]: [
                    {
                        "uuid": eng["uuid"],
                        "user": self._mo_values["uuid"],
                        "primary_boolean": eng["is_primary"],
                        "from_date": eng["validity"]["from"],
                        "to_date": eng["validity"]["to"],
                        "extensions": extensions,
                    }
                ]
                for eng in self._mo_engagements
            }
        else:
            return {
                "engagement_uuid": [
                    {
                        "uuid": "engagement_uuid",
                        "user": self._mo_values["uuid"],
                        "primary_boolean": True,
                        "from_date": "1960-01-01",
                        "to_date": None,
                        "extensions": extensions,
                        # Additional keys read by `AdLifeCycle._gen_filtered_employees`
                        "job_function": None,
                        "primary_type": None,
                        "engagement_type": None,
                    }
                ]
            }


class MockLoraCacheExtended(MockLoraCache):
    """Mocks enough of `LoraCache` to test `AdLifeCycle`"""

    def populate_cache(self, **kwargs):
        pass

    def calculate_derived_unit_data(self):
        pass

    def calculate_primary_engagements(self):
        pass

    @property
    def units(self):
        # Return a single org unit (= the root org unit)
        return {
            MO_ROOT_ORG_UNIT_UUID: [
                {
                    "uuid": MO_ROOT_ORG_UNIT_UUID,
                }
            ]
        }

    @property
    def classes(self):
        return {None: {}}

    def _load_settings(self):
        return {}

    def _read_org_uuid(self):
        return "not-a-mo-org-uuid"


class MockUnknownCPRADParameterReader(MockADParameterReader):
    def read_user(self, cpr=None, **kwargs):
        if cpr == UNKNOWN_CPR_NO:
            return None
        return super().read_user(cpr=cpr, **kwargs)


class MockMoraHelper(MoraHelper):
    def __init__(self, cpr):
        self._mo_user = {"cpr_no": cpr, "uuid": MO_UUID}
        super().__init__()

    def read_organisation(self):
        return "not-a-org-uuid"

    def read_all_users(self):
        return [self._mo_user]
