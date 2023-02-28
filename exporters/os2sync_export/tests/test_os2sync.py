from unittest.mock import patch
from uuid import uuid4

from os2sync_export.os2sync_models import OrgUnit

from .helpers import dummy_settings

uuid = uuid4()
o = OrgUnit(name="test", uuid=uuid, parentOrgUnitUuid=None)
o2 = OrgUnit(
    name="test",
    uuid=uuid,
    parentOrgUnitUuid=None,
    LOSShortName="Some losShortName",
    payoutUnitUuid=uuid4(),
    contactPlaces=[uuid4()],
    tasks=[uuid4()],
)


@patch("os2sync_export.config.get_os2sync_settings", return_value=dummy_settings)
def test_os2sync_upsert_org_unit_no_changes(get_settings_mock):
    """Test that if there are no changes to an org_unit we won't write to os2sync"""
    from os2sync_export.os2sync import upsert_org_unit

    with patch("os2sync_export.os2sync.os2sync_get_org_unit", return_value=o):
        with patch("os2sync_export.os2sync.os2sync_post") as post_mock:
            upsert_org_unit(o, "os2sync_api_url")
            post_mock.assert_not_called()


@patch("os2sync_export.config.get_os2sync_settings", return_value=dummy_settings)
def test_os2sync_upsert_org_unit_new(get_settings_mock):
    """Test that if no orgUnit was found in fk-org we create it."""
    from os2sync_export.os2sync import upsert_org_unit

    with patch("os2sync_export.os2sync.os2sync_get_org_unit", return_value=None):
        with patch("os2sync_export.os2sync.os2sync_post") as post_mock:
            upsert_org_unit(o, "os2sync_api_url")
            post_mock.assert_called_once_with("{BASE}/orgUnit/", json=o.json())


@patch("os2sync_export.config.get_os2sync_settings", return_value=dummy_settings)
def test_os2sync_upsert_org_unit_changes(get_settings_mock):
    """If there are changes to an orgunit it is sent to os2sync"""
    from os2sync_export.os2sync import upsert_org_unit

    org_unit = o.copy()
    with patch("os2sync_export.os2sync.os2sync_get_org_unit", return_value=o.copy()):
        with patch("os2sync_export.os2sync.os2sync_post") as post_mock:
            org_unit.Name = "Changed name"
            upsert_org_unit(org_unit, "os2sync_api_url")
            post_mock.assert_called_once_with("{BASE}/orgUnit/", json=org_unit.json())


@patch("os2sync_export.config.get_os2sync_settings", return_value=dummy_settings)
def test_os2sync_upsert_org_unit_keep_fk_fields(get_settings_mock):
    """Test that certain fields are fetched from fk-org. If these fields are found we use their values in the payload"""
    from os2sync_export.os2sync import upsert_org_unit

    with patch("os2sync_export.os2sync.os2sync_get_org_unit", return_value=o2):
        with patch("os2sync_export.os2sync.os2sync_post") as post_mock:

            upsert_org_unit(o, "os2sync_api_url")

            post_mock.assert_not_called()


@patch("os2sync_export.config.get_os2sync_settings", return_value=dummy_settings)
def test_os2sync_upsert_org_unit_changes_w_fixed_fields(get_settings_mock):
    """Test that values from fk-org is kept even if there are changes to an orgunit"""
    from os2sync_export.os2sync import upsert_org_unit

    org_unit = o.copy()
    fk_org = o2.copy()
    with patch("os2sync_export.os2sync.os2sync_get_org_unit", return_value=fk_org):
        with patch("os2sync_export.os2sync.os2sync_post") as post_mock:
            org_unit.Name = "Changed name"
            expected = o.copy()
            expected.Name = org_unit.Name
            upsert_org_unit(org_unit, "os2sync_api_url")
            post_mock.assert_called_once_with("{BASE}/orgUnit/", json=expected.json())
