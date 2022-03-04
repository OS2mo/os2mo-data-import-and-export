from integrations.SD_Lon.engagement import is_external
from integrations.SD_Lon.engagement import is_employment_id_and_no_salary_minimum_consistent
from collections import OrderedDict
from integrations.SD_Lon.tests.fixtures import get_read_employment_changed_fixture

import unittest


class TestIsExternal(unittest.TestCase):
    def test_is_external_is_false_for_number_employment_id(self):
        assert not is_external("12345")
        assert not is_external("1")

    def test_is_external_is_true_for_non_number_employment_id(self):
        assert is_external("ABCDE")


class TestIsEmploymentIdAndNoSalaryMinimumConsistent(unittest.TestCase):
    def setUp(self) -> None:
        self.employment = OrderedDict([
            ('EmploymentIdentifier', 'ABCDE'),
            ('EmploymentDate', '2020-01-01'),
            ('AnniversaryDate', '2020-01-01'),
            ('EmploymentDepartment', OrderedDict([
                ('@changedAtDate', '2020-11-10'),
                ('ActivationDate', '2020-01-01'),
                ('DeactivationDate', '9999-12-31'),
                ('DepartmentIdentifier', 'department_id'),
                ('DepartmentUUIDIdentifier', 'department_uuid')
            ])),
            ('Profession', OrderedDict([
                ('@changedAtDate', '2020-11-10'),
                ('ActivationDate', '2020-01-01'),
                ('DeactivationDate', '9999-12-31'),
                ('JobPositionIdentifier', '8000'),
                ('EmploymentName', 'Employment name'),
                ('AppointmentCode', '0')
            ])),
            ('EmploymentStatus', OrderedDict([
                ('@changedAtDate', '2020-11-10'),
                ('ActivationDate', '2020-01-01'),
                ('DeactivationDate', '9999-12-31'),
                ('EmploymentStatusCode', '1')
            ]))
        ])

    def test_true_when_no_salary_minimum_is_None(self):
        assert is_employment_id_and_no_salary_minimum_consistent(
            self.employment, None
        )
