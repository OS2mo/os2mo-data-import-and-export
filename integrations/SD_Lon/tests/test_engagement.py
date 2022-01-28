from datetime import datetime
from collections import OrderedDict

from parameterized import parameterized

from integrations.SD_Lon.engagement import get_employment_from_date


@parameterized.expand(
    [
        (False, datetime(2022, 2, 22)),
        (True, datetime(2011, 11, 11))
    ]
)
def test_get_from_date(use_activation_date, date):
    employment = OrderedDict([
        ('EmploymentDate', '2011-11-11'),
        ('EmploymentStatus', OrderedDict([
            ('ActivationDate', '2022-02-22')]
        ))
    ])

    from_date = get_employment_from_date(employment, use_activation_date)

    assert from_date == date
