NICKNAME_TEMPLATE = "{% if nickname -%}{{ nickname }}{%- else %}{{ name }}{%- endif %}"


class MoEmployeeMixin:
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

    def mock_employee_response(self, **kwargs):
        mo_employee = self.mock_employee(**kwargs)

        class MockResponse:
            def json(self):
                return mo_employee

        return MockResponse()
