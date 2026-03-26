import os
from typing import Any
from typing import cast
from unittest.mock import MagicMock
from unittest.mock import patch
from uuid import UUID

from click.testing import CliRunner
from fastramqpi.raclients.graph.client import GraphQLClient
from more_itertools import last
from more_itertools import one
from pydantic import AnyHttpUrl
from pydantic import SecretStr

from reports.safetynet.config import SafetyNetSettings
from reports.safetynet.config import SourceSystem
from reports.safetynet.safetynet import main
from reports.safetynet.safetynet import process_adm_unit


@patch("reports.safetynet.safetynet.get_unified_settings")
@patch("reports.safetynet.safetynet.get_mo_client")
def test_adm_engagements_opus_ou_manager_not_the_same_as_eng_employee(
    mock_get_mo_client: MagicMock,
    mock_get_unified_settings: MagicMock,
    opus_ou_manager_not_the_same_as_eng_employee,
    engagements,
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
        engagements,
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
def test_adm_engagements_opus_ou_manager_the_same_as_eng_employee(
    mock_get_mo_client: MagicMock,
    mock_get_unified_settings: MagicMock,
    opus_ou_manager_the_same_as_eng_employee,
    engagements,
    opus_ou_parent_manager: dict[str, Any],
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
        engagements,
        # The parent manager GraphQL call
        opus_ou_parent_manager,
    ]
    mock_get_mo_client.return_value = mock_gql_client

    runner = CliRunner()

    # Act
    runner.invoke(main, ["--skip-upload", "--only-adm-org"])

    # Assert
    with open(csv_file) as fp:
        csv_lines = fp.readlines()
    os.remove(csv_file)

    parent_ou_call = last(mock_gql_client.execute.call_args_list)
    assert parent_ou_call[1] == {
        "variable_values": {"org_unit": "9d1af806-f4d6-44e2-a001-a5deb3aa6703"}
    }

    assert csv_lines == [
        "Medarbejdernummer||CPR||Fornavn||Efternavn||Mail||Afdelingskode||Startdato||Slutdato||LedersMedarbejdernummer||Brugernavn||Titel||Faggruppe\n",
        "12345||0101011255||Bruce||Lee||bruce@kung.fu||9d1af806-f4d6-44e2-a001-a5deb3aa6703||2021-10-22||2025-09-30||54321||bruce||Kung Fu Master||Kung Fu Master\n",
    ]


@patch("reports.safetynet.safetynet.get_mo_client")
def test_csv_adm_engagements_sd_ou_manager_not_the_same_as_eng_employee(
    mock_get_mo_client: MagicMock,
    sd_ou_manager_not_the_same_as_eng_employee,
    engagements,
) -> None:
    # Arrange
    settings = SafetyNetSettings(
        auth_server=cast(AnyHttpUrl, "http://mocked.keycloak"),
        client_id="client-id",
        client_secret=SecretStr("secret"),
        mora_base="http://mora.base",
        safetynet_adm_unit_uuid=UUID("9d1af806-f4d6-44e2-a001-a5deb3aa6703"),
        source_system=SourceSystem.SD,
    )

    mock_gql_client = MagicMock(spec=GraphQLClient)
    mock_gql_client.execute.side_effect = [
        # The OU GraphQL call
        sd_ou_manager_not_the_same_as_eng_employee,
        # The engagements GraphQL call
        engagements,
    ]
    mock_get_mo_client.return_value = mock_gql_client

    # Act
    adm_eng_rows, _ = process_adm_unit(
        gql_client=mock_gql_client,
        settings=settings,
        org_unit=UUID("9d1af806-f4d6-44e2-a001-a5deb3aa6703"),
        adm_eng_rows=[],
        adm_ou_rows=[],
    )

    # Assert
    assert one(adm_eng_rows).manager_eng_user_key == "54321"


@patch("reports.safetynet.safetynet.get_mo_client")
def test_manager_engagement_coupling(
    mock_get_mo_client: MagicMock,
    sd_ou_manager_not_the_same_as_eng_employee,
    engagements_with_multiple_manager_engagements,
) -> None:
    # Arrange
    settings = SafetyNetSettings(
        auth_server=cast(AnyHttpUrl, "http://mocked.keycloak"),
        client_id="client-id",
        client_secret=SecretStr("secret"),
        mora_base="http://mora.base",
        safetynet_adm_unit_uuid=UUID("9d1af806-f4d6-44e2-a001-a5deb3aa6703"),
        source_system=SourceSystem.SD,
    )

    mock_gql_client = MagicMock(spec=GraphQLClient)
    mock_gql_client.execute.side_effect = [
        # The OU GraphQL call
        sd_ou_manager_not_the_same_as_eng_employee,
        # The engagements GraphQL call
        engagements_with_multiple_manager_engagements,
    ]
    mock_get_mo_client.return_value = mock_gql_client

    # Act
    adm_eng_rows, _ = process_adm_unit(
        gql_client=mock_gql_client,
        settings=settings,
        org_unit=UUID("9d1af806-f4d6-44e2-a001-a5deb3aa6703"),
        adm_eng_rows=[],
        adm_ou_rows=[],
    )

    # Assert
    assert one(adm_eng_rows).manager_eng_user_key == "54321"


@patch("reports.safetynet.safetynet.get_unified_settings")
@patch("reports.safetynet.safetynet.get_mo_client")
def test_csv_adm_engagements_sd_ou_manager_not_the_same_as_eng_employee_integration(
    mock_get_mo_client: MagicMock,
    mock_get_unified_settings: MagicMock,
    sd_ou_manager_not_the_same_as_eng_employee,
    engagements,
) -> None:
    # Arrange
    csv_file = "/tmp/adm-engagements.csv"

    mock_get_unified_settings.return_value = SafetyNetSettings(
        auth_server=cast(AnyHttpUrl, "http://mocked.keycloak"),
        client_id="client-id",
        client_secret=SecretStr("secret"),
        mora_base="http://mora.base",
        safetynet_adm_unit_uuid=UUID("9d1af806-f4d6-44e2-a001-a5deb3aa6703"),
        source_system=SourceSystem.SD,
    )

    mock_gql_client = MagicMock(spec=GraphQLClient)
    mock_gql_client.execute.side_effect = [
        # The OU GraphQL call
        sd_ou_manager_not_the_same_as_eng_employee,
        # The engagements GraphQL call
        engagements,
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
def test_adm_engagements_sd_ou_manager_cpr_included(
    mock_get_mo_client: MagicMock,
    mock_get_unified_settings: MagicMock,
    sd_ou_manager_not_the_same_as_eng_employee,
    engagements,
) -> None:
    # Arrange
    csv_file = "/tmp/adm-engagements.csv"

    mock_get_unified_settings.return_value = SafetyNetSettings(
        auth_server=cast(AnyHttpUrl, "http://mocked.keycloak"),
        client_id="client-id",
        client_secret=SecretStr("secret"),
        mora_base="http://mora.base",
        safetynet_adm_unit_uuid=UUID("9d1af806-f4d6-44e2-a001-a5deb3aa6703"),
        source_system=SourceSystem.SD,
        include_manager_cpr=True,
    )

    mock_gql_client = MagicMock(spec=GraphQLClient)
    mock_gql_client.execute.side_effect = [
        # The OU GraphQL call
        sd_ou_manager_not_the_same_as_eng_employee,
        # The engagements GraphQL call
        engagements,
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
        "Medarbejdernummer||CPR||Fornavn||Efternavn||Mail||Afdelingskode||Startdato||Slutdato||LedersMedarbejdernummer||LedersCPR||Brugernavn||Titel||Faggruppe\n",
        "12345||0101011255||Bruce||Lee||bruce@kung.fu||9d1af806-f4d6-44e2-a001-a5deb3aa6703||2021-10-22||2025-09-30||54321||2001011299||bruce||Kung Fu Master||Kung Fu Master\n",
    ]
