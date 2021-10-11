import unittest
from uuid import uuid4

from parameterized import parameterized

from exporters.os2rollekatalog.os2rollekatalog_integration import (
    get_parent_org_unit_uuid,
)


class MockOU:
    "Mock class for api-object"

    def __init__(self, uuid, parent_uuid):
        parent_json = {"uuid": str(parent_uuid)} if parent_uuid else None
        self.json = {"parent": parent_json}
        self.uuid = uuid


root = uuid4()
limit = uuid4()
testparent = uuid4()

class RollekatalogTestUnits(unittest.TestCase):
    @parameterized.expand(
        [
        #No ou filtering:
            #Unit is actual root and returns None
            (None,root, False, None),
            #unit is not root but has no parent, returns root
            (None,uuid4(), False, root),
            #unit has a parent, parent is returned
            (testparent,uuid4(), False, testparent),
            
        #Filtering is on. 
            # Root unit still returns None as parent
            (None, root, True, None),
            #unit has a parent, parent is returned
            (testparent,uuid4(), True, testparent),
        ]
    )
    def test_get_parent(self, parent_uuid,uuid, limit, expected):
        mocked_ou = MockOU(uuid, parent_uuid)
        res = get_parent_org_unit_uuid(mocked_ou, limit, root)
        self.assertEqual(res, expected)
        
    @parameterized.expand(
        [
        #Filtering is on
            #Unit has a no parent and is not the root
            #this should never happen because it should have been filtered. 
            #Assert that an exception is raised 
            (None, uuid4(), True, None),
        ]
    )
    def test_assert_filtered(self, parent_uuid,uuid, limit, expected):
        mocked_ou = MockOU(uuid, parent_uuid)
        with self.assertRaises(AssertionError):
            res = get_parent_org_unit_uuid(mocked_ou, limit, root)
