import unittest

from parameterized import parameterized

from integrations.os2sync.templates import FieldTemplateRenderError
from integrations.os2sync.templates import FieldTemplateSyntaxError
from integrations.os2sync.templates import Person
from integrations.os2sync.tests.helpers import MoEmployeeMixin
from integrations.os2sync.tests.helpers import NICKNAME_TEMPLATE


class TestPerson(unittest.TestCase, MoEmployeeMixin):
    @parameterized.expand(
        [
            # OS2SYNC_XFER_CPR, key of expected CPR value
            (True, "cpr_no"),
            (False, None),
        ]
    )
    def test_transfer_cpr(self, flag, expected_key):
        mo_employee = self.mock_employee()
        person = Person(mo_employee, settings={"OS2SYNC_XFER_CPR": flag})
        expected_cpr = mo_employee.get(expected_key)
        self.assertDictEqual(
            person.to_json(),
            {"Name": mo_employee["name"], "Cpr": expected_cpr},
        )


class TestPersonNameTemplate(unittest.TestCase, MoEmployeeMixin):
    @parameterized.expand(
        [
            (
                {"nickname": False},  # mo employee response kwargs
                "name",  # key of expected value for `Name`
            ),
            (
                {"nickname": True},  # mo employee response kwargs
                "nickname",  # key of expected value for `Name`
            ),
        ]
    )
    def test_template(self, response_kwargs, expected_key):
        mo_employee = self.mock_employee(**response_kwargs)
        person = Person(mo_employee, settings=self._gen_settings(NICKNAME_TEMPLATE))
        self.assertEqual(person.to_json()["Name"], mo_employee[expected_key])

    def test_template_syntax_error_raises_exception(self):
        mo_employee = self.mock_employee()
        with self.assertRaises(FieldTemplateSyntaxError):
            Person(mo_employee, settings=self._gen_settings("{% invalid jinja %}"))

    @parameterized.expand(
        [
            "{{ name|dictsort }}"
            "{{ unknown_variable }}"
        ]
    )
    def test_template_render_failure_raises_exception(self, template):
        mo_employee = self.mock_employee()
        person = Person(mo_employee, settings=self._gen_settings(template))
        with self.assertRaises(FieldTemplateRenderError):
            person.to_json()

    def _gen_settings(self, template):
        return {
            "OS2SYNC_TEMPLATES": {"person.name": template},
            "OS2SYNC_XFER_CPR": True,  # required by `Person.to_json`
        }
