import unittest
from uuid import uuid4

from parameterized import parameterized

from exporters.os2rollekatalog.config import RollekatalogSettings
from exporters.os2rollekatalog.os2rollekatalog_integration import RollekatalogsExporter


class MockOU:
    "Mock class for api-object"

    def __init__(self, uuid, parent_uuid):
        parent_json = {"uuid": str(parent_uuid)} if parent_uuid else None
        self.json = {"parent": parent_json}
        self.uuid = uuid


def get_mocked_ou(uuid, parent_uuid):
    parent_json = {"uuid": str(parent_uuid)} if parent_uuid else None
    return {"uuid": str(uuid), "parent": parent_json}


root = uuid4()
limit = uuid4()
testparent = uuid4()


class RollekatalogTestUnits(unittest.TestCase, RollekatalogsExporter):
    @parameterized.expand(
        [
            # No ou filtering:
            # Unit is actual root and returns None
            (None, root, False, None),
            # unit is not root but has no parent, returns root
            (None, uuid4(), False, str(root)),
            # unit has a parent, parent is returned
            (testparent, uuid4(), False, str(testparent)),
            # Filtering is on.
            # Root unit still returns None as parent
            (None, root, True, None),
            # unit has a parent, parent is returned
            (testparent, uuid4(), True, str(testparent)),
        ]
    )
    def test_get_parent(self, parent_uuid, uuid, limit, expected):
        mocked_ou = get_mocked_ou(uuid, parent_uuid)
        self.settings = RollekatalogSettings(
            exporters_os2rollekatalog_ou_filter=limit,
            exporters_os2rollekatalog_main_root_org_unit=root,
        )
        res = self.get_parent_org_unit_uuid(mocked_ou)
        self.assertEqual(res, expected)

    @parameterized.expand(
        [
            # Filtering is on
            # Unit has a no parent and is not the root
            # this should never happen because it should have been filtered.
            # Assert that an exception is raised
            (None, uuid4(), True, None),
        ]
    )
    def test_assert_filtered(self, parent_uuid, uuid, limit, _):
        self.settings = RollekatalogSettings(
            exporters_os2rollekatalog_ou_filter=limit,
            exporters_os2rollekatalog_main_root_org_unit=root,
        )
        mocked_ou = get_mocked_ou(uuid, parent_uuid)
        with self.assertRaises(AssertionError):
            self.get_parent_org_unit_uuid(mocked_ou)
