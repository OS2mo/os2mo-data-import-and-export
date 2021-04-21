import unittest

from integrations.os2sync.templates import FieldTemplateRenderError
from integrations.os2sync.templates import FieldTemplateSyntaxError
from integrations.os2sync.templates import Person


class _EmployeeMixin:
    def mock_employee(self, nickname=False):
        # Mock the result of `os2mo_get("{BASE}/e/" + uuid + "/").json()`
        # Only contains the keys relevant for testing
        return {
            # Name
            "name": "Test Testesen",
            "givenname": "Test",
            "surname": "Testesen",
            # Nickname
            "nickname": "Kalde Navn" if nickname else "",
            "nickname_givenname": "Kalde" if nickname else "",
            "nickname_surname": "Navn" if nickname else "",
            # Other fields
            "cpr_no": "0101019999",
            "user_key": "testtestesen",
            "uuid": "mock-uuid"
        }


class TestPerson(unittest.TestCase, _EmployeeMixin):
    def test_transfer_cpr_true(self):
        mo_employee = self.mock_employee()
        person = Person(mo_employee, settings={"OS2SYNC_XFER_CPR": True})
        self.assertDictEqual(
            person.to_json(),
            {"Name": mo_employee["name"], "Cpr": mo_employee["cpr_no"]},
        )

    def test_transfer_cpr_false(self):
        mo_employee = self.mock_employee()
        person = Person(mo_employee, settings={"OS2SYNC_XFER_CPR": False})
        self.assertDictEqual(
            person.to_json(),
            {"Name": mo_employee["name"], "Cpr": None},
        )


class TestPersonNameTemplate(unittest.TestCase, _EmployeeMixin):
    settings = {
        "OS2SYNC_TEMPLATES": {
            "person.name":
            "{% if nickname -%}{{ nickname }}{%- else %}{{ name }}{%- endif %}"
        },
        "OS2SYNC_XFER_CPR": True,  # required by `Person.to_json`
    }

    def test_nickname_present(self):
        mo_employee = self.mock_employee(nickname=True)
        person = Person(mo_employee, settings=self.settings)
        self.assertEqual(person.to_json()["Name"], mo_employee["nickname"])

    def test_nickname_absent(self):
        mo_employee = self.mock_employee(nickname=False)
        person = Person(mo_employee, settings=self.settings)
        self.assertEqual(person.to_json()["Name"], mo_employee["name"])


class TestPersonInvalidTemplate(unittest.TestCase, _EmployeeMixin):
    def test_syntax_error_raises_exception(self):
        settings = {
            "OS2SYNC_TEMPLATES": {
                "person.name": "{% invalid jinja %}"
            },
            "OS2SYNC_XFER_CPR": True,  # required by `Person.to_json`
        }
        mo_employee = self.mock_employee()
        with self.assertRaises(FieldTemplateSyntaxError):
            Person(mo_employee, settings=settings)

    def test_render_failure_raises_exception(self):
        settings = {
            "OS2SYNC_TEMPLATES": {
                "person.name": "{{ name|dictsort }}"
            },
            "OS2SYNC_XFER_CPR": True,  # required by `Person.to_json`
        }
        mo_employee = self.mock_employee()
        person = Person(mo_employee, settings=settings)
        with self.assertRaises(FieldTemplateRenderError):
            person.to_json()

    def test_unknown_variable_raises_exception(self):
        settings = {
            "OS2SYNC_TEMPLATES": {
                "person.name": "{{ unknown_variable }}"
            },
            "OS2SYNC_XFER_CPR": True,  # required by `Person.to_json`
        }
        mo_employee = self.mock_employee()
        person = Person(mo_employee, settings=settings)
        with self.assertRaises(FieldTemplateRenderError):
            person.to_json()
