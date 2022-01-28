from datetime import datetime
from collections import OrderedDict

from integrations.SD_Lon.engagement import get_from_date

employment = OrderedDict([
    ('EmploymentDate', '2011-11-11'),
    ('EmploymentStatus', OrderedDict([
        ('ActivationDate', '2022-02-22')]
    ))
])


def test_return_activation_date():
    from_date = get_from_date(employment, False)
    assert from_date == datetime(2022, 2, 22)


def test_return_employment_date():
    from_date = get_from_date(employment, True)
    assert from_date == datetime(2011, 11, 11)

