import os
from typing import Any
from typing import cast
from unittest.mock import MagicMock
from unittest.mock import patch
from uuid import UUID

from click.testing import CliRunner
from fastramqpi.raclients.graph.client import GraphQLClient
from pydantic import AnyHttpUrl
from pydantic import SecretStr

from reports.safetynet.config import SafetyNetSettings
from reports.safetynet.safetynet import main


@patch("reports.safetynet.safetynet.get_unified_settings")
@patch("reports.safetynet.safetynet.get_mo_client")
def test_adm_engagements(
    mock_get_mo_client: MagicMock,
    mock_get_unified_settings: MagicMock,
    opus_ou_manager_not_the_same_as_eng_employee,
    opus_engagements: dict[str, Any],
) -> None:
    # Arrange
    csv_file = "/tmp/adm-engagements.csv"

    mock_get_unified_settings.return_value = SafetyNetSettings(
        auth_server=cast(AnyHttpUrl, "http://mocked.keycloak"),
        client_id="client-id",
        client_secret=SecretStr("secret"),
        mora_base="http://mora.base",
        safetynet_adm_unit_uuid=UUID("9d1af806-f4d6-44e2-a001-a5deb3aa6703"),
    )

    mock_gql_client = MagicMock(spec=GraphQLClient)
    mock_gql_client.execute.side_effect = [
        # The OU GraphQL call
        opus_ou_manager_not_the_same_as_eng_employee,
        # The engagements GraphQL call
        opus_engagements,
    ]
    mock_get_mo_client.return_value = mock_gql_client

    runner = CliRunner()

    # Act
    runner.invoke(main, ["--skip-upload", "--only-adm-org"])

    # Assert
    with open(csv_file) as fp:
        csv_lines = fp.readlines()
    os.remove(csv_file)

    assert csv_lines == [
        "Medarbejdernummer||CPR||Fornavn||Efternavn||Mail||Afdelingskode||Startdato||Slutdato||LedersMedarbejdernummer||Brugernavn||Titel||Faggruppe\n",
        "12345||0101011255||Bruce||Lee||bruce@kung.fu||9d1af806-f4d6-44e2-a001-a5deb3aa6703||2021-10-22||2025-09-30||54321||bruce||Kung Fu Master||Kung Fu Master\n",
    ]


@patch("reports.safetynet.safetynet.get_unified_settings")
@patch("reports.safetynet.safetynet.get_mo_client")
def test_adm_engagements_get_managers_recursively(
    mock_get_mo_client: MagicMock,
    mock_get_unified_settings: MagicMock,
    opus_ou_manager_the_same_as_eng_employee,
    opus_engagements: dict[str, Any],
    opus_ou_parent_manager_same_as_eng_employee: dict[str, Any],
    opus_ou_grand_parent_manager_not_same_as_eng_employee: dict[str, Any],
) -> None:
    # Arrange
    csv_file = "/tmp/adm-engagements.csv"

    mock_get_unified_settings.return_value = SafetyNetSettings(
        auth_server=cast(AnyHttpUrl, "http://mocked.keycloak"),
        client_id="client-id",
        client_secret=SecretStr("secret"),
        mora_base="http://mora.base",
        safetynet_adm_unit_uuid=UUID("9d1af806-f4d6-44e2-a001-a5deb3aa6703"),
    )

    mock_gql_client = MagicMock(spec=GraphQLClient)
    mock_gql_client.execute.side_effect = [
        # The OU GraphQL call
        opus_ou_manager_the_same_as_eng_employee,
        # The engagements GraphQL call
        opus_engagements,
        # The parent manager GraphQL call
        opus_ou_parent_manager_same_as_eng_employee,
        # The grandparent manager GraphQL call
        opus_ou_grand_parent_manager_not_same_as_eng_employee,
    ]
    mock_get_mo_client.return_value = mock_gql_client

    runner = CliRunner()

    # Act
    runner.invoke(main, ["--skip-upload", "--only-adm-org"])

    # Assert
    with open(csv_file) as fp:
        csv_lines = fp.readlines()
    os.remove(csv_file)

    parent_ou_calls = mock_gql_client.execute.call_args_list[2:]
    assert parent_ou_calls[0][1] == {
        "variable_values": {"org_unit": "9d1af806-f4d6-44e2-a001-a5deb3aa6703"}
    }
    assert parent_ou_calls[1][1] == {
        "variable_values": {"org_unit": "48a3819e-c5e1-4f4d-b4ad-582caff6ea51"}
    }

    assert csv_lines == [
        "Medarbejdernummer||CPR||Fornavn||Efternavn||Mail||Afdelingskode||Startdato||Slutdato||LedersMedarbejdernummer||Brugernavn||Titel||Faggruppe\n",
        "12345||0101011255||Bruce||Lee||bruce@kung.fu||9d1af806-f4d6-44e2-a001-a5deb3aa6703||2021-10-22||2025-09-30||54321||bruce||Kung Fu Master||Kung Fu Master\n",
    ]
