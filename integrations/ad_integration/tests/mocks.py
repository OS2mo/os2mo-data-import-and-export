from ..ad_writer import MORESTSource


class MockMORESTSource(MORESTSource):
    def __init__(self, from_date, to_date):
        self.from_date = from_date
        self.to_date = to_date

    def get_engagement_dates(self, uuid):
        # Return 2-tuple of (from_dates, to_dates)
        return [self.from_date], [self.to_date]
