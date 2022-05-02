from unittest import TestCase

from ..ad_writer import MORESTSource
from .mocks import MO_UUID
from .mocks import MockMoraHelper


class _MockMoraHelperMultipleEmails(MockMoraHelper):
    def get_e_addresses(self, e_uuid, scope=None):
        # Duplicate the single-element list returned by `get_e_adddresses`
        return super().get_e_addresses(e_uuid, scope) * 2


class _TestableMORESTSource(MORESTSource):
    def __init__(self, helper):
        self.helper = helper


class TestMORESTSource(TestCase):
    def test_get_email_address_handles_multiple_addresses(self):
        # Regression test: `MORESTSource.get_email_address` crashed on employees with
        # multiple email addresses.
        instance = _TestableMORESTSource(_MockMoraHelperMultipleEmails("cpr"))
        addr = instance.get_email_address(MO_UUID)
        # Assert we receive a single dictionary
        self.assertIsInstance(addr, dict)
