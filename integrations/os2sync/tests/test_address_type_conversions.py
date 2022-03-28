import unittest

from integrations.os2sync.os2mo import addresses_to_orgunit


class _AddressMixin:
    def mock_address_list(self, scope, user_key, value):
        # Mock the result of
        # `os2mo_get("{BASE}/ou/" + uuid + "/details/address").json()`
        # Only contains the keys relevant for `addresses_to_orgunit`
        return [
            {
                "address_type": {
                    "scope": scope,
                    "user_key": user_key,
                },
                "name": value,
            }
        ]


class TestContactOpenHours(unittest.TestCase, _AddressMixin):
    def test_contact_open_hours(self):
        result = {}
        mo_data = self.mock_address_list(
            "TEXT", "ContactOpenHours", "Man-fre: 11-13.30"
        )
        addresses_to_orgunit(result, mo_data)  # Mutates `result`
        self.assertDictEqual(result, {"ContactOpenHours": "Man-fre: 11-13.30"})


class TestDtrId(unittest.TestCase, _AddressMixin):
    def test_dtr_id(self):
        result = {}
        mo_data = self.mock_address_list("TEXT", "DtrId", "G123456")
        addresses_to_orgunit(result, mo_data)  # Mutates `result`
        self.assertDictEqual(result, {"DtrId": "G123456"})
