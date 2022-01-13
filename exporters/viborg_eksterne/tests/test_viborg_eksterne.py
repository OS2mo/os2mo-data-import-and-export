from unittest import TestCase
from unittest.mock import MagicMock

from ..viborg_eksterne import ViborgEksterne


class MockLoRaCache:
    _user_uuid = "user-uuid"
    _user_name = "Ansat Ansatsen"
    _user_cpr = "1282121234"

    _manager_uuid = "manager-uuid"
    _manager_name = "Leder Ledersen"

    _org_unit_uuid = "org-unit-uuid"
    _org_unit_name = "Enhedsnavn"

    _engagement_uuid = "engagement-uuid"
    _engagement_user_key = "engagement-user-key"
    _engagement_from_date = "2020-01-01"

    _engagement_type_uuid = "engagement-type-uuid"
    _engagement_type_title = "engagement-type-title"

    _address_uuid = "address-uuid"
    _address_scope = "E-mail"
    _address_value = "leder@organisation.kommune"

    @property
    def users(self):
        employee = {
            "uuid": self._user_uuid,
            "navn": self._user_name,
            "cpr": self._user_cpr,
        }
        manager = {
            "uuid": self._manager_uuid,
            "navn": self._manager_name,
        }
        return {
            self._user_uuid: [employee],
            self._manager_uuid: [manager],
        }

    @property
    def units(self):
        unit = {
            "name": self._org_unit_name,
            "acting_manager_uuid": self._manager_uuid,
        }
        return {self._org_unit_uuid: [unit]}

    @property
    def engagements(self):
        engagement = {
            "uuid": self._engagement_uuid,
            "user": self._user_uuid,
            "unit": self._org_unit_uuid,
            "engagement_type": self._engagement_type_uuid,
            "user_key": self._engagement_user_key,
            "from_date": self._engagement_from_date,
        }
        return {self._engagement_uuid: [engagement]}

    @property
    def managers(self):
        manager = {
            "user": self._manager_uuid,
        }
        return {self._manager_uuid: [manager]}

    @property
    def addresses(self):
        address = {
            "uuid": self._address_uuid,
            "user": self._manager_uuid,
            "scope": self._address_scope,
            "value": self._address_value,
        }
        return {self._address_uuid: [address]}

    @property
    def classes(self):
        return {}


class MockLoraCacheHistoric(MockLoRaCache):
    @property
    def classes(self):
        _class = {
            "title": self._engagement_type_title,
        }
        return {self._engagement_type_uuid: _class}


class TestableViborgEksterne(ViborgEksterne):
    def _read_settings(self):
        return {
            "exporters.plan2learn.allowed_engagement_types": [],
        }


class TestExportEngagement(TestCase):
    def setUp(self):
        super().setUp()
        self._instance = TestableViborgEksterne()

    def test_export_engagement_using_loracache(self):
        # Arrange
        mh = MagicMock()
        lc = MockLoRaCache()
        lc_historic = MockLoraCacheHistoric()

        # Act
        self._instance.export_engagement(mh, "filename", lc, lc_historic)

        # Assert
        mh._write_csv.assert_called_once_with(
            self._instance.fieldnames,
            [
                {
                    "OrganisationsenhedUUID": MockLoRaCache._org_unit_uuid,
                    "Organisationsenhed": MockLoRaCache._org_unit_name,
                    "Ledernavn": MockLoRaCache._manager_name,
                    "Lederemail": MockLoRaCache._address_value,
                    "Tjenestenummer": MockLoRaCache._engagement_user_key,
                    "CPR-nummer": MockLoRaCache._user_cpr,
                    "Navn": MockLoRaCache._user_name,
                    "Engagementstype": MockLoRaCache._engagement_type_title,
                    "Startdato": "%s 00:00:00" % MockLoRaCache._engagement_from_date,
                }
            ],
            "filename",
        )
