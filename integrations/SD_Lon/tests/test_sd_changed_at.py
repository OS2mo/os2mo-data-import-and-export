import unittest
import xmltodict
from collections import OrderedDict
from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, patch

from parameterized import parameterized

from integrations.SD_Lon.sd_changed_at import ChangeAtSD
from integrations.ad_integration.utils import AttrDict


@patch("integrations.SD_Lon.sd_changed_at.primary_types", autospec=True)
@patch("integrations.SD_Lon.sd_changed_at.MOPrimaryEngagementUpdater", autospec=True)
@patch("integrations.SD_Lon.sd_changed_at.FixDepartments", autospec=True)
@patch("integrations.SD_Lon.sd_changed_at.MoraHelper", autospec=True)
@patch("integrations.SD_Lon.sd_changed_at.load_settings", autospec=True)
class Test_sd_changed_at(unittest.TestCase):
    def setUp(self):
        pass

    def setup_settings_mock(self, load_settings):
        load_settings.return_value = {
            'integrations.SD_Lon.job_function': 'JobPositionIdentifier',
            'integrations.SD_Lon.use_ad_integration': False,
            'mora.base': 'dummy',
        }

    def read_person_fixture(self, cpr, first_name, last_name, employment_id):
        institution_id = "XX"

        sd_request_reply = AttrDict({
            "text": """
            <GetPerson20111201 creationDateTime="2020-12-03T17:40:10">
                <RequestStructure>
                    <InstitutionIdentifier>""" + institution_id + """</InstitutionIdentifier>
                    <PersonCivilRegistrationIdentifier>""" + cpr + """</PersonCivilRegistrationIdentifier>
                    <EffectiveDate>2020-12-03</EffectiveDate>
                    <StatusActiveIndicator>true</StatusActiveIndicator>
                    <StatusPassiveIndicator>false</StatusPassiveIndicator>
                    <ContactInformationIndicator>false</ContactInformationIndicator>
                    <PostalAddressIndicator>false</PostalAddressIndicator>
                </RequestStructure>
                <Person>
                    <PersonCivilRegistrationIdentifier>""" + cpr + """</PersonCivilRegistrationIdentifier>
                    <PersonGivenName>""" + first_name + """</PersonGivenName>
                    <PersonSurnameName>""" + last_name + """</PersonSurnameName>
                    <Employment>
                        <EmploymentIdentifier>""" + employment_id + """</EmploymentIdentifier>
                    </Employment>
                </Person>
            </GetPerson20111201>
            """
        })

        expected_read_person_result = [OrderedDict([
            ('PersonCivilRegistrationIdentifier', cpr),
            ('PersonGivenName', first_name),
            ('PersonSurnameName', last_name),
            ('Employment', OrderedDict([('EmploymentIdentifier', employment_id)]))
        ])]

        return sd_request_reply, expected_read_person_result

    @patch("integrations.SD_Lon.sd_common._sd_request")
    def test_read_person(self, sd_request, load_settings, *args):
        """Test that read_person does the expected transformation."""
        self.setup_settings_mock(load_settings)

        cpr = "0101709999"
        sd_reply, expected_read_person_result = self.read_person_fixture(
            cpr = cpr,
            first_name = "John",
            last_name = "Deere",
            employment_id = "1337"
        )

        sd_request.return_value = sd_reply

        today = date.today()
        tomorrow = today + timedelta(days=1)
        sd_updater = ChangeAtSD(today, tomorrow)
        result = sd_updater.read_person(cpr=cpr)
        self.assertEqual(result, expected_read_person_result)

    def test_update_changed_persons(self, load_settings, morahelper, *args):
        self.setup_settings_mock(load_settings)
        cpr = "0101709999"
        first_name = "John"
        last_name = "Deere"
        employment_id = "1337"

        morahelper.return_value.read_organisation.return_value = "org_uuid"
        morahelper.return_value.read_user.return_value.__getitem__.return_value = "user_uuid"

        _, read_person_result = self.read_person_fixture(
            cpr = cpr,
            first_name = first_name,
            last_name = last_name,
            employment_id = employment_id
        )

        today = date.today()
        tomorrow = today + timedelta(days=1)
        sd_updater = ChangeAtSD(today, tomorrow)

        sd_updater.read_person = lambda cpr: read_person_result

        _mo_post = morahelper.return_value._mo_post
        self.assertFalse(_mo_post.called)
        sd_updater.update_changed_persons(cpr=cpr)
        _mo_post.assert_called_with(
            'e/create', {
                'givenname': first_name,
                'surname': last_name,
                'cpr_no': cpr,
                'org': {'uuid': 'org_uuid'},
                'uuid': 'user_uuid'
            }
        )
