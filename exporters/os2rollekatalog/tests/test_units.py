import unittest
from uuid import uuid4

from parameterized import parameterized

from exporters.os2rollekatalog.os2rollekatalog_integration import (
    get_parent_org_unit_uuid,
)


class MockOU:
    "Mock class for api-object"

    def __init__(self, value):
        self.json = value
        self.uuid = uuid4()


root = uuid4()
limit = uuid4()
testparent = uuid4()

class RollekatalogTestUnits(unittest.TestCase):
    @parameterized.expand(
        [
            # If there are no parent allways use root uuid
            ({"parent": None}, limit, root),
            ({"parent": None}, None, root),
            # If there is a parent use it.
            ({"parent": {"uuid": str(testparent)}}, limit, testparent),
            ({"parent": {"uuid": str(testparent)}}, None, testparent),
            # If there is a limit set and the parent is this limit, use root uuid.
            ({"parent": {"uuid": str(limit)}}, limit, root),
        ]
    )
    def test_get_parent(self, ou, limit, expected):
        mocked_ou = MockOU(ou)
        res = get_parent_org_unit_uuid(mocked_ou, limit, root)
        self.assertEqual(res, expected)
