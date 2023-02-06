import unittest
from unittest.mock import MagicMock
from unittest.mock import patch
from uuid import UUID
from uuid import uuid4

from hypothesis import given
from hypothesis import strategies as st
from os2sync_export.os2mo import get_org_unit_hierarchy
from os2sync_export.os2mo import get_work_address
from os2sync_export.os2mo import is_ignored
from os2sync_export.os2mo import kle_to_orgunit
from os2sync_export.os2mo import org_unit_uuids
from os2sync_export.os2mo import os2mo_get
from os2sync_export.os2mo import overwrite_position_uuids
from os2sync_export.os2mo import overwrite_unit_uuids
from os2sync_export.os2mo import partition_kle
from parameterized import parameterized
from tests.helpers import dummy_settings
from tests.helpers import MockOs2moGet


class TestsMOAd(unittest.TestCase):
    def test_is_ignored(self):
        settings = dummy_settings
        il1, il2, iu1, iu2 = [uuid4() for u in range(4)]
        settings.os2sync_ignored_unit_levels = [il1, il2]
        settings.os2sync_ignored_unit_types = [iu1, iu2]
        unit = {
            "org_unit_level": {"uuid": str(uuid4())},
            "org_unit_type": {"uuid": str(uuid4())},
        }
        self.assertFalse(is_ignored(unit, settings))
        unit = {
            "org_unit_level": {"uuid": str(uuid4())},
            "org_unit_type": {"uuid": str(iu2)},
        }
        self.assertTrue(is_ignored(unit, settings))
        unit = {
            "org_unit_level": {"uuid": str(il1)},
            "org_unit_type": {"uuid": str(uuid4())},
        }
        self.assertTrue(is_ignored(unit, settings))

    @parameterized.expand(
        [
            ({"os2mo_has_kle": True}, ["2", "4", "6"], []),
            (
                {"os2mo_has_kle": True, "os2sync_use_contact_for_tasks": True},
                ["2", "4"],
                ["6"],
            ),
        ]
    )
    def test_kle_to_orgunit(
        self, testsettings, expected_tasks, expected_contactfortasks
    ):

        kles = [
            {
                "uuid": 1,
                "kle_aspect": [{"name": "Udførende"}],
                "kle_number": {"uuid": "2"},
            },
            {
                "uuid": 3,
                "kle_aspect": [{"name": "Udførende"}],
                "kle_number": {"uuid": "4"},
            },
            {
                "uuid": 5,
                "kle_aspect": [{"name": "Ansvarlig"}],
                "kle_number": {"uuid": "6"},
            },
        ]
        settings = dummy_settings
        settings.os2sync_use_contact_for_tasks = (
            True if testsettings.get("os2sync_use_contact_for_tasks") else False
        )

        tasks, contact_for_tasks = partition_kle(
            kles, settings.os2sync_use_contact_for_tasks
        )
        self.assertListEqual(expected_tasks, tasks)
        self.assertListEqual(expected_contactfortasks, contact_for_tasks)
        org_unit = {}
        kle_to_orgunit(
            org_unit, kles, use_contact_for_tasks=settings.os2sync_use_contact_for_tasks
        )
        self.assertListEqual(expected_tasks, org_unit.get("Tasks"))
        self.assertListEqual(
            expected_contactfortasks, org_unit.get("ContactForTasks", [])
        )

    @parameterized.expand(
        [
            (["Henvendelsessted", "Adresse"], "Henvendelsesstednavn"),
            (["Adresse", "Henvendelsessted"], "Adressenavn"),
            ([], None),
        ]
    )
    def test_get_work_address(self, work_address_names, expected):
        addresses = [
            {
                "name": "Henvendelsesstednavn",
                "address_type": {"name": "Henvendelsessted"},
            },
            {"name": "Adressenavn", "address_type": {"name": "Adresse"}},
            {"name": "Adressenavn2", "address_type": {"name": "Adresse"}},
        ]
        positions = [{"is_primary": True, "OrgUnitUuid": "Some_unit_uuid"}]
        with patch(
            "os2sync_export.os2mo.os2mo_get", return_value=MockOs2moGet(addresses)
        ):

            work_address = get_work_address(positions, work_address_names)
            self.assertEqual(work_address, expected)

    @parameterized.expand(
        [
            # No (relevant) it systems - no change
            ([], [], {"Uuid": "old_uuid", "ParentOrgUnitUuid": "old_parent_uuid"}),
            (
                [{"itsystem": {"name": "irrelevant it system"}, "user_key": "dummy"}],
                [{"itsystem": {"name": "irrelevant it system"}, "user_key": "dummy"}],
                {"Uuid": "old_uuid", "ParentOrgUnitUuid": "old_parent_uuid"},
            ),
            # Overwrite both uuid and parent uuid
            (
                [{"itsystem": {"name": "FK-org uuid"}, "user_key": "fk-unit_uuid"}],
                [{"itsystem": {"name": "FK-org uuid"}, "user_key": "parent_uuid"}],
                {"Uuid": "fk-unit_uuid", "ParentOrgUnitUuid": "parent_uuid"},
            ),
            (
                [{"itsystem": {"name": "AD ObjectGUID"}, "user_key": "fk-unit_uuid"}],
                [{"itsystem": {"name": "AD ObjectGUID"}, "user_key": "parent_uuid"}],
                {"Uuid": "fk-unit_uuid", "ParentOrgUnitUuid": "parent_uuid"},
            ),
            # Two it-systems - use first from the given list (fk-org first, AD second)
            (
                [
                    {"itsystem": {"name": "FK-org uuid"}, "user_key": "right_uuid"},
                    {"itsystem": {"name": "AD ObjectGUID"}, "user_key": "wrong_uuid"},
                ],
                [],
                {"Uuid": "right_uuid", "ParentOrgUnitUuid": "old_parent_uuid"},
            ),
            (
                [],
                [
                    {
                        "itsystem": {"name": "FK-org uuid"},
                        "user_key": "right_parent_uuid",
                    },
                    {
                        "itsystem": {"name": "AD ObjectGUID"},
                        "user_key": "wrong_parent_uuid",
                    },
                ],
                {"Uuid": "old_uuid", "ParentOrgUnitUuid": "right_parent_uuid"},
            ),
        ]
    )
    def test_overwrite_unit_uuids(self, it_system, parent_it_system, expected):
        test_org = {"Uuid": "old_uuid", "ParentOrgUnitUuid": "old_parent_uuid"}
        with patch(
            "os2sync_export.os2mo.os2mo_get",
            side_effect=[MockOs2moGet(it_system), MockOs2moGet(parent_it_system)],
        ):
            overwrite_unit_uuids(test_org, ["FK-org uuid", "AD ObjectGUID"])
        assert test_org == expected

    @parameterized.expand(
        [
            # No (relevant) it systems - no change
            (
                [],
                {"Uuid": "old_uuid", "Positions": [{"OrgUnitUuid": "mo_unit_uuid"}]},
            ),
            # Person has a position in a unit with a mapped uuid:
            (
                [
                    {
                        "itsystem": {"name": "AD ObjectGUID"},
                        "user_key": "new_uuid",
                    },
                ],
                {"Uuid": "old_uuid", "Positions": [{"OrgUnitUuid": "new_uuid"}]},
            ),
        ]
    )
    def test_overwrite_position_uuids(self, position_it_systems, expected):
        test_user = {
            "Uuid": "old_uuid",
            "Positions": [{"OrgUnitUuid": "mo_unit_uuid"}],
        }
        with patch(
            "os2sync_export.os2mo.os2mo_get",
            side_effect=[MockOs2moGet(position_it_systems)],
        ):
            overwrite_position_uuids(test_user, ["FK-org uuid", "AD ObjectGUID"])
        assert test_user == expected


@patch("os2sync_export.os2mo.organization_uuid", return_value="root_uuid")
@given(st.tuples(st.uuids()))
def test_org_unit_uuids(root_mock, hierarchy_uuids):
    session_mock = MagicMock()
    os2mo_get.cache_clear()
    org_unit_uuids.cache_clear()
    with patch("os2sync_export.os2mo.os2mo_get") as session_mock:

        session_mock.return_value = MockOs2moGet({"items": [{"uuid": "test"}]})
        org_unit_uuids(hierarchy_uuids=hierarchy_uuids)

    session_mock.assert_called_once_with(
        "{BASE}/o/root_uuid/ou/",
        limit=999999,
        hierarchy_uuids=tuple(str(u) for u in hierarchy_uuids),
    )


@patch("os2sync_export.os2mo.organization_uuid", return_value="root_uuid")
def test_get_org_unit_hierarchy(root_mock):

    os2mo_get.cache_clear()

    with patch(
        "os2sync_export.os2mo.os2mo_get",
        return_value=MockOs2moGet(
            {
                "uuid": "403eb28f-e21e-bdd6-3612-33771b098a12",
                "user_key": "org_unit_hierarchy",
                "description": "",
                "data": {
                    "total": 2,
                    "offset": 0,
                    "items": [
                        {
                            "uuid": "8c30ab5a-8c3a-566c-bf12-790bdd7a9fef",
                            "name": "Skjult organisation",
                            "user_key": "hide",
                            "example": None,
                            "scope": "TEXT",
                            "owner": None,
                        },
                        {
                            "uuid": "f805eb80-fdfe-8f24-9367-68ea955b9b9b",
                            "name": "Linjeorganisation",
                            "user_key": "linjeorg",
                            "example": None,
                            "scope": "TEXT",
                            "owner": None,
                        },
                    ],
                },
                "path": "/o/3b866d97-0b1f-48e0-8078-686d96f430b3/f/org_unit_hierarchy/",
            }
        ),
    ):
        hierarchy_uuids = get_org_unit_hierarchy("Linjeorganisation")
    assert hierarchy_uuids == (UUID("f805eb80-fdfe-8f24-9367-68ea955b9b9b"),)
