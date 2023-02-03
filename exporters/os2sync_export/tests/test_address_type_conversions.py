import unittest
from unittest.mock import patch
from uuid import uuid4

import pytest
from os2sync_export.os2mo import addresses_to_orgunit
from os2sync_export.os2mo import addresses_to_user


class _AddressMixin:
    def mock_address_list(self, scope, user_key, value, uuid=str(uuid4())):
        # Mock the result of
        # `os2mo_get("{BASE}/ou/" + uuid + "/details/address").json()`
        # Only contains the keys relevant for `addresses_to_orgunit`
        return [
            {
                "address_type": {
                    "uuid": uuid,
                    "scope": scope,
                    "user_key": user_key,
                },
                "name": value,
                "uuid": uuid4(),
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


class TestAddressesToUser(unittest.TestCase):
    def test_scope_uuid_conversion(self):
        """When calling `choose_public_address`, `addresses_to_user` must convert its
        `phone_scope_classes` and `email_scope_classes` arguments from lists of UUIDs
        to lists of strings. Otherwise, the first address with the expected scope is
        returned, regardless of its address type UUID.

        See: #50169
        """
        user = {}
        addresses = []
        phone_scope_classes = [uuid4()]
        landline_scope_classes = [uuid4()]
        email_scope_classes = [uuid4()]
        with patch("os2sync_export.os2mo.choose_public_address") as mock_choose:
            # Mutates `result`
            addresses_to_user(
                user,
                addresses,
                phone_scope_classes,
                landline_scope_classes,
                email_scope_classes,
            )
            # Assert lists of UUIDs are converted to lists of strings before calling
            # `choose_public_address`
            pairs = zip(
                mock_choose.call_args_list,
                [landline_scope_classes, phone_scope_classes, email_scope_classes],
            )
            for call, class_uuid_list in pairs:
                self.assertEqual(call.args, ([], list(map(str, class_uuid_list))))


def get_dummy_addresses():
    address_generator = _AddressMixin()
    phone = address_generator.mock_address_list(
        "PHONE", "phone", "phonenumber", uuid="phone_uuid"
    )[0]
    landline = address_generator.mock_address_list(
        "PHONE", "landline", "landlinenumber", uuid="landline_uuid"
    )[0]
    email = address_generator.mock_address_list(
        "EMAIL", "email", "someone@email.com", uuid="email_uuid"
    )[0]
    return [phone, landline, email]


def test_get_user_addresses_default():
    """With no default, pick email and phone from scope
    With no priority set we can't know which phonenumber will be used
    """
    user = {}
    addresses = get_dummy_addresses()
    addresses_to_user(user, addresses)
    assert user["Email"] == "someone@email.com"
    assert user["PhoneNumber"] in ("phonenumber", "landlinenumber")


@pytest.mark.parametrize(
    "settings_dict,expected",
    [
        (
            # Prioritize phonenumber
            {
                "phone_scope_classes": ["phone_uuid", "landline_uuid"],
            },
            {
                "Email": "someone@email.com",
                "PhoneNumber": "phonenumber",
            },
        ),
        (
            # Prioritize landline
            {
                "phone_scope_classes": ["landline_uuid", "phone_uuid"],
            },
            {
                "Email": "someone@email.com",
                "PhoneNumber": "landlinenumber",
            },
        ),
        (
            # Use landline field when configured
            {
                "phone_scope_classes": ["phone_uuid"],
                "landline_scope_classes": ["landline_uuid"],
                "email_scope_classes": ["email_uuid"],
            },
            {
                "Email": "someone@email.com",
                "PhoneNumber": "phonenumber",
                "Landline": "landlinenumber",
            },
        ),
    ],
)
def test_get_user_addresses(settings_dict, expected):
    user = {}
    addresses = get_dummy_addresses()
    addresses_to_user(user, addresses, **settings_dict)
    assert user == expected
