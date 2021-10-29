from pathlib import Path
from unittest.mock import patch

from lxml import etree
from parameterized import parameterized

from integrations.SD_Lon.sd_log_analyzer import extract_log_file_lines
from integrations.SD_Lon.sd_log_analyzer import get_all_sd_person_changes
from integrations.SD_Lon.sd_log_analyzer import get_sd_person_changed
from integrations.SD_Lon.sd_log_analyzer import get_sd_person_changed_at_date_responses
from integrations.SD_Lon.sd_log_analyzer import get_sd_xml_responses
from integrations.SD_Lon.sd_log_analyzer import get_tar_gz_archive_files
from integrations.SD_Lon.sd_log_analyzer import IdType
from integrations.SD_Lon.sd_log_analyzer import output_to_file
from integrations.SD_Lon.sd_log_analyzer import SdPersonChange

remove_blank_parser = etree.XMLParser(remove_blank_text=True)

XML_FIXTURE = etree.XML(
    """
    <GetPersonChangedAtDate20111201>
        <RequestStructure>
            <InstitutionIdentifier>XY</InstitutionIdentifier>
            <ActivationDate>2021-09-14</ActivationDate>
            <ActivationTime>00:00:00</ActivationTime>
            <DeactivationDate>2021-09-15</DeactivationDate>
            <DeactivationTime>00:00:59</DeactivationTime>
            <ContactInformationIndicator>false</ContactInformationIndicator>
            <PostalAddressIndicator>false</PostalAddressIndicator>
        </RequestStructure>
        <Person>
            <PersonCivilRegistrationIdentifier>1111111111</PersonCivilRegistrationIdentifier>
            <PersonGivenName>Bruce</PersonGivenName>
            <PersonSurnameName>Lee</PersonSurnameName>
            <Employment>
                <EmploymentIdentifier>11111</EmploymentIdentifier>
            </Employment>
        </Person>
        <Person>
            <PersonCivilRegistrationIdentifier>2222222222</PersonCivilRegistrationIdentifier>
            <PersonGivenName>Chuck</PersonGivenName>
            <PersonSurnameName>Norris</PersonSurnameName><Employment>
                <EmploymentIdentifier>22222</EmploymentIdentifier>
            </Employment>
        </Person>
    </GetPersonChangedAtDate20111201>
    """,
    parser=remove_blank_parser,
)

BRUCE_LEE = etree.XML(
    """
    <Person>
        <PersonCivilRegistrationIdentifier>1111111111</PersonCivilRegistrationIdentifier>
        <PersonGivenName>Bruce</PersonGivenName>
        <PersonSurnameName>Lee</PersonSurnameName>
        <Employment>
            <EmploymentIdentifier>11111</EmploymentIdentifier>
        </Employment>
    </Person>
    """,
    parser=remove_blank_parser,
)


def get_xml_responses_from_tar_gz_file():
    tar_gz_file = Path(
        "integrations/SD_Lon/tests/fixtures/2021-09-21-06-06-55-cron-backup.tar.gz"
    )
    log_file_lines = extract_log_file_lines(tar_gz_file)
    xml_responses = get_sd_xml_responses(log_file_lines)
    return xml_responses


class TestGetTarGzArchiveFiles:
    def test_returns_tar_gz_files_in_folder(self):
        path = Path("integrations/SD_Lon/tests/fixtures")
        tar_gz_files = get_tar_gz_archive_files(path)

        assert len(tar_gz_files) == 2
        assert "2021-09-21-06-06-55-cron-backup.tar.gz" == tar_gz_files[0].name
        assert "2021-09-22-06-06-55-cron-backup.tar.gz" == tar_gz_files[1].name

    def test_returns_empty_list_when_no_tar_files_in_folder(self):
        path = Path("integrations/SD_Lon/tests")
        tar_gz_files = get_tar_gz_archive_files(path)

        assert [] == tar_gz_files


class TestExtractLogFileLines:
    @patch("tarfile.TarFile.extractfile")
    def test_log_file_does_not_exist(self, mock_tarfile):
        mock_tarfile.side_effect = KeyError()

        assert [] == extract_log_file_lines(
            Path(
                "integrations/SD_Lon/tests/fixtures/"
                "2021-09-21-06-06-55-cron-backup.tar.gz"
            )
        )


class TestGetSdXmlResponses:
    def test_log_file_should_contain_two_elements(self):
        xml_responses = get_xml_responses_from_tar_gz_file()

        assert isinstance(xml_responses, list)
        assert len(xml_responses) == 2
        assert etree.iselement(xml_responses[0])
        assert etree.iselement(xml_responses[1])

    def test_return_empty_list_when_no_sd_xml_responses(self):
        log_file_lines = ["no", "sd", "responses"]
        xml_responses = get_sd_xml_responses(log_file_lines)

        assert [] == xml_responses


class TestGetSdPersonChangedAtDateResponses:
    def test_should_contain_a_single_person_changed_at_date_response(self):
        xml_responses = get_xml_responses_from_tar_gz_file()
        changed_at_date_responses = get_sd_person_changed_at_date_responses(
            xml_responses
        )

        assert isinstance(changed_at_date_responses, list)
        assert len(changed_at_date_responses) == 1

        assert etree.tostring(XML_FIXTURE) == etree.tostring(
            changed_at_date_responses[0]
        )


class TestGetPerson:
    @parameterized.expand([(IdType.CPR, "1111111111"), (IdType.EMPLOYMENT_ID, "11111")])
    def test_get_person_from_identifier(self, id_type, id_):
        actual_person = get_sd_person_changed(id_type, id_, XML_FIXTURE).change

        assert etree.tostring(BRUCE_LEE) == etree.tostring(actual_person)

    def test_return_none_when_no_persons_exists(self):
        xml_root = etree.XML(
            """
            <GetPersonChangedAtDate20111201>
                <RequestStructure>
                    <InstitutionIdentifier>XY</InstitutionIdentifier>
                    <ActivationDate>2021-09-14</ActivationDate>
                    <ActivationTime>00:00:00</ActivationTime>
                    <DeactivationDate>2021-09-15</DeactivationDate>
                    <DeactivationTime>00:00:59</DeactivationTime>
                    <ContactInformationIndicator>false</ContactInformationIndicator>
                    <PostalAddressIndicator>false</PostalAddressIndicator>
                </RequestStructure>
            </GetPersonChangedAtDate20111201>
            """,
            parser=remove_blank_parser,
        )

        actual = get_sd_person_changed(IdType.EMPLOYMENT_ID, "11111", xml_root).change

        assert actual is None

    def test_request_date_timestamps_set_correctly(self):
        sd_person_changed = get_sd_person_changed(
            IdType.EMPLOYMENT_ID, "11111", XML_FIXTURE
        )

        assert "2021-09-14" == sd_person_changed.start_date
        assert "2021-09-15" == sd_person_changed.end_date


class TestGetAllSdPersonChanges:
    def test_get_sd_person_changes_from_all_tar_gz_files(self):
        all_changes = get_all_sd_person_changes(
            IdType.EMPLOYMENT_ID, "11111", Path("integrations/SD_Lon/tests/fixtures")
        )

        assert "2021-09-14" == all_changes[0].start_date
        assert "2021-09-15" == all_changes[0].end_date
        assert etree.tostring(BRUCE_LEE) == etree.tostring(all_changes[0].change)

        # The same SD person is used in the fixture for convenience
        assert "2021-09-19" == all_changes[1].start_date
        assert "2021-09-20" == all_changes[1].end_date
        assert etree.tostring(BRUCE_LEE) == etree.tostring(all_changes[1].change)


class TestOutputToFile:
    @patch("builtins.open")
    @patch("lxml.etree.tostring")
    def test_skip_tars_with_no_changes(self, mock_etree, mock_open):
        all_changes = [
            SdPersonChange(
                start_date="not used here", end_date="not used here", change=BRUCE_LEE
            ),
            SdPersonChange(start_date="not used here", end_date="not used here"),
            SdPersonChange(
                start_date="not used here", end_date="not used here", change=BRUCE_LEE
            ),
        ]

        output_to_file(all_changes, Path("/tmp/dipex-writeable-file.txt"))

        assert 2 == mock_etree.call_count
