from ..ad_writer import MORESTSource
from .test_utils import TestADWriterMixin


class MockADParameterReader(TestADWriterMixin):
    def read_user(self, cpr=None, **kwargs):
        return self._prepare_get_from_ad(ad_transformer=None)

    def read_it_all(self, **kwargs):
        return [self.read_user()]


class MockMORESTSource(MORESTSource):
    def __init__(self, from_date, to_date):
        self.from_date = from_date
        self.to_date = to_date

    def get_engagement_dates(self, uuid):
        # Return 2-tuple of (from_dates, to_dates)
        return [self.from_date], [self.to_date]
