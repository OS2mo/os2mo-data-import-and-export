import uuid
from unittest import TestCase
from unittest.mock import patch

import pytest

from ..ad_writer import EngagementDatesError
from ..ad_writer import LoraCacheSource
from ..ad_writer import MOGraphqlSource
from ..ad_writer import MORESTSource
from ..utils import AttrDict
from .mocks import MockMORESTSource


class TestMOGraphqlSource:
    def _get_instance(self, *objs) -> MOGraphqlSource:
        wrapped_value = [{"objects": [obj]} for obj in objs]
        with patch(
            "integrations.ad_integration.ad_writer.MOGraphqlSource._run_query",
            return_value=wrapped_value,
        ):
            return MOGraphqlSource({})

    def _get_mock_engagement(self, employee_uuid, is_primary=True, end=None):
        return {
            "employee_uuid": employee_uuid,
            "is_primary": is_primary,
            "validity": {"to": end.isoformat() if end else None},
        }

    def _get_manager_uuid(self, instance, employee_uuid):
        return instance.get_manager_uuid({"uuid": employee_uuid}, None)

    def test_employee_has_manager_in_same_org_unit(self):
        employee_uuid = str(uuid.uuid4())
        manager_uuid = str(uuid.uuid4())
        instance = self._get_instance(
            {
                "uuid": str(uuid.uuid4()),  # org unit UUID
                "parent_uuid": None,
                "engagements": [self._get_mock_engagement(employee_uuid)],
                "managers": [{"employee_uuid": manager_uuid}],
            }
        )
        assert self._get_manager_uuid(instance, employee_uuid) == manager_uuid

    def test_employee_is_self_manager(self):
        org_unit_uuid = str(uuid.uuid4())
        parent_org_unit_uuid = str(uuid.uuid4())
        employee_uuid = str(uuid.uuid4())
        manager_uuid = str(uuid.uuid4())
        instance = self._get_instance(
            # Child org unit: where the employee is engaged and also is a "self manager"
            {
                "uuid": org_unit_uuid,
                "parent_uuid": parent_org_unit_uuid,
                "engagements": [self._get_mock_engagement(employee_uuid)],
                "managers": [{"employee_uuid": employee_uuid}],  # "self manager"
            },
            # Parent org unit: where the "real" manager is defined
            {
                "uuid": parent_org_unit_uuid,
                "parent_uuid": None,
                "engagements": [],
                "managers": [{"employee_uuid": manager_uuid}],  # the "real" manager
            },
        )
        assert self._get_manager_uuid(instance, employee_uuid) == manager_uuid

    def test_employee_is_self_manager_no_parent_managers(self):
        employee_uuid = str(uuid.uuid4())
        instance = self._get_instance(
            {
                "uuid": str(uuid.uuid4()),  # org unit UUID
                "parent_uuid": None,
                "engagements": [self._get_mock_engagement(employee_uuid)],
                "managers": [{"employee_uuid": employee_uuid}],
            }
        )
        assert self._get_manager_uuid(instance, employee_uuid) is None

    def test_employee_has_no_primary_engagement(self):
        employee_uuid = str(uuid.uuid4())
        instance = self._get_instance(
            {
                "uuid": str(uuid.uuid4()),  # org unit UUID
                "parent_uuid": None,
                "engagements": [
                    self._get_mock_engagement(employee_uuid, is_primary=None)
                ],
                "managers": [{"employee_uuid": str(uuid.uuid4())}],
            }
        )
        assert self._get_manager_uuid(instance, employee_uuid) is None

    def test_employee_has_no_engagements(self):
        employee_uuid = str(uuid.uuid4())
        instance = self._get_instance(
            {
                "uuid": str(uuid.uuid4()),  # org unit UUID
                "parent_uuid": None,
                "engagements": [],
                "managers": [{"employee_uuid": str(uuid.uuid4())}],
            }
        )
        assert self._get_manager_uuid(instance, employee_uuid) is None

    def test_employee_has_no_managers(self):
        employee_uuid = str(uuid.uuid4())
        instance = self._get_instance(
            {
                "uuid": str(uuid.uuid4()),  # org unit UUID
                "parent_uuid": None,
                "engagements": [self._get_mock_engagement(employee_uuid)],
                "managers": [],
            }
        )
        assert self._get_manager_uuid(instance, employee_uuid) is None

    def test_follows_pagination(self):
        """Verify that `MOGraphqlSource._run_query` follows the pagination cursors in
        the responses returned by the GraphQL API.
        """

        class _MockClient:
            """Mock GraphQL client whose `execute` method simulates a paginated set of
            results.
            """

            def __init__(self):
                self.num_pages = 5
                self.current_page = 0

            def __enter__(self):
                # Implement the context manager protocol, returning the instance itself
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                # Implement the context manager protocol,
                pass

            def execute(self, query, **kwargs):
                response = {
                    "org_units": {
                        "page_info": {
                            "next_cursor": (
                                str(self.current_page)
                                if self.current_page < self.num_pages
                                else None
                            )
                        },
                        "objects": [],
                    }
                }
                if self.current_page < self.num_pages:
                    self.current_page += 1
                return response

        # Arrange: test `MOGraphqlSource` using our mock GraphQL client
        mock_client = _MockClient()
        with patch(
            "integrations.ad_integration.ad_writer.MOGraphqlSource._get_client",
            return_value=mock_client,
        ):
            instance = MOGraphqlSource({})
            # Act: consume the paginated result set
            instance._run_query()
            # Assert: check that we reached the last page of the paginated result set.
            # (We assume that we also visited the other pages in the result set.)
            assert mock_client.current_page == mock_client.num_pages


class TestLoraCacheSource(TestCase):
    def setUp(self):
        self.user = self.setup_user()
        self.lc = AttrDict(
            {
                "users": {
                    self.user["uuid"]: [self.user],
                },
                "engagements": {
                    "key-1": [
                        # Current registration
                        {
                            "user": self.user["uuid"],
                            "from_date": "2020-01-01",
                            "to_date": None,
                        },
                        # Previous registration
                        {
                            "user": self.user["uuid"],
                            "from_date": "2019-01-01",
                            "to_date": "2021-01-01",
                        },
                    ],
                },
            }
        )
        self.lc_historic = AttrDict(
            {
                "engagements": {
                    # Add some historic engagements that must not be read by
                    # `get_engagement_dates`.
                    "key-1": [
                        # Current historic registration
                        {
                            "user": self.user["uuid"],
                            "from_date": "2010-01-01",
                            "to_date": "2019-12-31",
                        },
                    ]
                }
            }
        )

    def setup_user(self):
        return {
            "uuid": "some_uuid_here",
            "navn": "some_name some_lastname",
            "efternavn": "some_lastname",
            "fornavn": "some_name",
            "kaldenavn": "some_alias some_lastname_alias",
            "kaldenavn_fornavn": "some_alias",
            "kaldenavn_efternavn": "some_lastname_alias",
            "cpr": "some_cpr",
        }

    def test_read_user(self):
        datasource = self._get_datasource(None, None)
        result = datasource.read_user("some_uuid_here")
        self.assertEqual(
            result,
            {
                "uuid": "some_uuid_here",
                "name": "some_name some_lastname",
                "surname": "some_lastname",
                "givenname": "some_name",
                "nickname": "some_alias some_lastname_alias",
                "nickname_givenname": "some_alias",
                "nickname_surname": "some_lastname_alias",
                "cpr_no": "some_cpr",
            },
        )

    def test_get_engagement_dates(self):
        datasource = self._get_datasource("2020-01-01", None)
        result = datasource.get_engagement_dates(self.user["uuid"])
        self.assertEqual(
            [list(elem) for elem in result],  # consume each iterable in result
            [["2020-01-01"], [None]],
        )

    def test_get_engagement_endpoint_dates(self):
        datasource = self._get_datasource("2020-01-01", None)
        result = datasource.get_engagement_endpoint_dates(self.user["uuid"])
        # "to_date" of None must be converted into "9999-12-31"
        self.assertEqual(result, ("2020-01-01", "9999-12-31"))

    def _get_datasource(self, from_date, to_date):
        return LoraCacheSource(
            self.lc, self.lc_historic, MockMORESTSource(from_date, to_date)
        )


class TestEngagementDates:
    MOSource = MORESTSource(settings={"global": {"mora.base": "http://lol.money:1337"}})

    def test_valid_interval(self, monkeypatch):
        start, end = "2021-01-01", "2021-02-02"

        def _valid_dates(*args):
            return [start], [end]

        monkeypatch.setattr(self.MOSource, "get_engagement_dates", _valid_dates)
        assert self.MOSource.get_engagement_endpoint_dates("test") == (start, end)

    def test_sentinels(self, monkeypatch):
        start, end = "1930-01-01", "9999-12-31"

        def _falsy_dates(*args):
            return [None], [""]

        monkeypatch.setattr(self.MOSource, "get_engagement_dates", _falsy_dates)
        assert self.MOSource.get_engagement_endpoint_dates("test") == (start, end)

    def test_invalid_dates(self, monkeypatch):
        start, end = "2021-12-31", "2021-01-01"

        def _invalid_dates(*args):
            return [start], [end]

        monkeypatch.setattr(self.MOSource, "get_engagement_dates", _invalid_dates)
        with pytest.raises(EngagementDatesError, match=f"{start}.*{end}"):
            self.MOSource.get_engagement_endpoint_dates("test")

        def _empty_dates(*args):
            return [], []

        monkeypatch.setattr(self.MOSource, "get_engagement_dates", _empty_dates)
        with pytest.raises(EngagementDatesError, match="9999-12-31.*1930-01-01"):
            self.MOSource.get_engagement_endpoint_dates("test")
