from unittest import TestCase

from parameterized import parameterized

from ..ad_writer import MORESTSource
from .mocks import MO_UUID
from .mocks import MockMoraHelper


class _MockMoraHelperMultipleEmails(MockMoraHelper):
    def get_e_addresses(self, e_uuid, scope=None):
        # Duplicate the single-element list returned by `get_e_adddresses`
        return super().get_e_addresses(e_uuid, scope) * 2


class _MockMoraHelperEmptyEmails(MockMoraHelper):
    def get_e_addresses(self, e_uuid, scope=None):
        return []


class _TestableMORESTSource(MORESTSource):
    def __init__(self, helper):
        self.helper = helper


class TestMORESTSource(TestCase):
    @parameterized.expand(
        [
            # Regression test: `MORESTSource.get_email_address` crashed on employees with
            # multiple email addresses.
            # See: #49971
            (_MockMoraHelperMultipleEmails, {"value": "address-value"}),
            # Regression test: `MORESTSource.get_email_address` crashed on employees with
            # no email addresses (empty list of addresses.)
            # See: #50325
            (_MockMoraHelperEmptyEmails, {}),
        ]
    )
    def test_get_email_address(self, mock_cls: MockMoraHelper, expected_addr: dict):
        instance = _TestableMORESTSource(mock_cls("cpr"))
        actual_addr = instance.get_email_address(MO_UUID)
        self.assertDictEqual(actual_addr, expected_addr)
