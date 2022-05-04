import logging
import unittest

from helpers import dummy_settings
from parameterized import parameterized

from integrations.os2sync.config import loggername as _loggername
from integrations.os2sync.templates import FieldTemplateRenderError
from integrations.os2sync.templates import FieldTemplateSyntaxError
from integrations.os2sync.templates import Person
from integrations.os2sync.tests.helpers import MoEmployeeMixin
from integrations.os2sync.tests.helpers import NICKNAME_TEMPLATE


class TestPerson(unittest.TestCase, MoEmployeeMixin):
    @parameterized.expand(
        [
            # mock CPR, os2sync_xfer_cpr, key of expected CPR value, expected log level
            ("0101013333", True, "cpr_no", logging.DEBUG),
            (None, True, "cpr_no", logging.WARNING),
            ("0101013333", False, None, logging.DEBUG),
            (None, False, None, logging.DEBUG),
        ]
    )
    def test_transfer_cpr(self, cpr, flag, expected_key, expected_log_level):
        mo_employee = self.mock_employee(cpr=cpr)
        settings = dummy_settings
        settings.os2sync_xfer_cpr = flag
        person = Person(mo_employee, settings=settings)
        expected_cpr = mo_employee.get(expected_key)
        with self.assertLogs(_loggername, expected_log_level):
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
            "{{ name|dictsort }}",
            "{{ unknown_variable }}",
        ]
    )
    def test_template_render_failure_raises_exception(self, template):
        mo_employee = self.mock_employee()
        person = Person(mo_employee, settings=self._gen_settings(template))
        with self.assertRaises(FieldTemplateRenderError):
            person.to_json()

    def _gen_settings(self, template):
        settings = dummy_settings
        settings.os2sync_xfer_cpr = True
        settings.os2sync_templates = {"person.name": template}
        return settings
