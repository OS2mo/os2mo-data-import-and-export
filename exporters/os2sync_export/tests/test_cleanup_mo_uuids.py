from unittest.mock import patch

import hypothesis.strategies as st
import pytest
from hypothesis import given
from tests.helpers import dummy_settings


@pytest.mark.parametrize("dry_run", [True, False])
@patch("os2sync_export.config.get_os2sync_settings", return_value=dummy_settings)
def test_remove_from_os2sync_noop(settings_mock, dry_run):
    # We need to import this after mocking get_os2sync_settings.
    from os2sync_export.cleanup_mo_uuids import remove_from_os2sync

    with patch("os2sync_export.cleanup_mo_uuids.get_it_user_uuids") as gql_mock:
        remove_from_os2sync(dummy_settings, dry_run=dry_run)
    gql_mock.assert_not_called()


@given(employees=st.lists(st.uuids()), org_units=st.lists(st.uuids()))
@patch("os2sync_export.config.get_os2sync_settings", return_value=dummy_settings)
def test_remove_from_os2sync_dry_run(settings_mock, employees, org_units):
    # We need to import this after mocking get_os2sync_settings.
    from os2sync_export.cleanup_mo_uuids import remove_from_os2sync

    # Assume all it-accounts are in the same it-system
    dummy_settings.os2sync_uuid_from_it_systems = ["it-system name"]
    it_system = {"user_key": dummy_settings.os2sync_uuid_from_it_systems[0]}

    # use given uuids to create a mock of the graphql return payload
    gql_response = [
        {
            "objects": [
                {
                    "employee_uuid": str(e_uuid),
                    "org_unit_uuid": None,
                    "itsystem": it_system,
                }
            ]
        }
        for e_uuid in employees
    ]
    gql_response.extend(
        [
            {
                "objects": [
                    {
                        "employee_uuid": None,
                        "org_unit_uuid": str(e_uuid),
                        "itsystem": it_system,
                    }
                ]
            }
            for e_uuid in org_units
        ]
    )

    # Act

    with patch(
        "os2sync_export.cleanup_mo_uuids.get_it_user_uuids", return_value=gql_response
    ) as gql_mock:
        org_unit_uuids, employee_uuids = remove_from_os2sync(
            dummy_settings, dry_run=True
        )

    # Assert
    gql_mock.assert_called_once_with(dummy_settings)
    assert set(employees) == employee_uuids
    assert set(org_units) == org_unit_uuids
