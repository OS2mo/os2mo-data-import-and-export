from datetime import date
from datetime import datetime
from uuid import uuid4

import los_files
import los_leder
from .helpers import mock_config
from unittest import mock
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
from los_files import FileSet


from .helpers import HelperMixin
from .strategies import csv_buf_from_model


class _TestableManagerImporter(los_leder.ManagerImporter):  # type: ignore
    """Defanged `ManagerImporter` with stub versions of `handle_{create,edit,terminate}`
    methods, as well as `cache_cpr`.
    """

    def __init__(self):
        super().__init__()
        self._handler_calls = []

    def cache_cpr(self):
        pass

    async def handle_create(self, filename: str, filedate: datetime):
        self._handler_calls.append(("handle_create", filename, filedate))

    async def handle_edit(self, filename: str, filedate: datetime):
        self._handler_calls.append(("handle_edit", filename, filedate))

    async def handle_terminate(self, filename: str, filedate: datetime):
        self._handler_calls.append(("handle_terminate", filename, filedate))


class TestParseManagerCreateCSV:
    @given(csv_buf_from_model(model=los_leder.ManagerCreate))
    def test_parse_raises_nothing(self, csv_buf):
        los_files.parse_csv(csv_buf.readlines(), los_leder.ManagerCreate)


class TestParseManagerEditCSV:
    @given(csv_buf_from_model(model=los_leder.ManagerEdit))
    def test_parse_raises_nothing(self, csv_buf):
        los_files.parse_csv(csv_buf.readlines(), los_leder.ManagerEdit)


class TestParseManagerTerminateCSV:
    @given(csv_buf_from_model(model=los_leder.ManagerTerminate))
    def test_parse_raises_nothing(self, csv_buf):
        los_files.parse_csv(csv_buf.readlines(), los_leder.ManagerTerminate)


class TestManagerImporter(HelperMixin):
    _employee_uuid = uuid4()

    @given(st.builds(los_leder.ManagerCreate), st.datetimes())
    def test_handle_create(
        self, instance: los_leder.ManagerCreate, filedate: datetime  # type: ignore
    ):
        importer, mock_csv, mock_session = self._setup(instance)
        with mock_csv, mock_session:
            with self._mock_create_details() as mock_create_details:
                self._run_until_complete(
                    importer.handle_create("unused_filename.csv", filedate)
                )
                self._assert_payload_contains(
                    mock_create_details,
                    {
                        "type": "manager",
                        "person": {"uuid": str(self._employee_uuid)},
                        "validity": {"from": date.today().isoformat(), "to": None},
                    },
                )

    @given(st.builds(los_leder.ManagerEdit), st.datetimes())
    def test_handle_edit_creates_new_detail(
        self, instance: los_leder.ManagerEdit, filedate: datetime  # type: ignore
    ):
        importer, mock_csv, mock_session = self._setup(instance)
        with mock_csv, mock_session:
            with self._mock_create_details() as mock_create_details:
                with self._mock_edit_details() as mock_edit_details:
                    with self._mock_lookup_organisationfunktion():
                        self._run_until_complete(
                            importer.handle_edit("unused_filename.csv", filedate)
                        )
                        self._assert_payload_contains(
                            mock_create_details,
                            {
                                "type": "manager",
                                "person": {"uuid": str(self._employee_uuid)},
                                "validity": {
                                    "from": date.today().isoformat(),
                                    "to": None,
                                },
                            },
                        )
                        self._assert_payload_empty(mock_edit_details)

    @given(st.builds(los_leder.ManagerEdit), st.datetimes())
    def test_handle_edit_updates_existing_detail(
        self, instance: los_leder.ManagerEdit, filedate: datetime  # type: ignore
    ):
        importer, mock_csv, mock_session = self._setup(instance)
        with mock_csv, mock_session:
            with self._mock_create_details() as mock_create_details:
                with self._mock_edit_details() as mock_edit_details:
                    orgfunk_uuid = importer._generate_rel_uuid(instance)
                    with self._mock_lookup_organisationfunktion(
                        return_value={str(orgfunk_uuid)}
                    ):
                        self._run_until_complete(
                            importer.handle_edit("unused_filename.csv", filedate)
                        )
                        self._assert_payload_empty(mock_create_details)
                        self._assert_payload_contains(
                            mock_edit_details,
                            {
                                "type": "manager",
                                "uuid": str(orgfunk_uuid),
                                "data": {
                                    "type": "manager",
                                    "uuid": str(orgfunk_uuid),
                                    "org_unit": {"uuid": str(instance.org_uuid)},
                                    "person": {"uuid": str(self._employee_uuid)},
                                    "validity": {
                                        "from": filedate.date().isoformat(),
                                    },
                                    "manager_level": {
                                        "uuid": str(instance.manager_level_uuid)
                                    },
                                    "manager_type": {
                                        "uuid": str(instance.manager_type_uuid)
                                    },
                                    "responsibility": [
                                        {"uuid": str(instance.responsibility_uuid)}
                                    ],
                                },
                            },
                        )

    @given(st.builds(los_leder.ManagerTerminate), st.datetimes())
    def test_handle_terminate(
        self, instance: los_leder.ManagerTerminate, filedate: datetime  # type: ignore
    ):
        importer, mock_csv, mock_session = self._setup(instance)
        with mock_csv, mock_session:
            with self._mock_terminate_details() as mock_terminate_details:
                self._run_until_complete(
                    importer.handle_terminate("unused_filename.csv", filedate)
                )
                self._assert_payload_contains(
                    mock_terminate_details,
                    {
                        "type": "manager",
                        "uuid": str(importer._generate_rel_uuid(instance)),
                        "validity": {"to": filedate.date().isoformat()},
                    },
                )

    @given(st.builds(los_leder.ManagerCreate))
    def test_generate_manager_payload_unmatched_cpr(self, instance):
        # Create importer with empty `cpr_cache`
        importer = los_leder.ManagerImporter()
        # Generate manager payload which will contain invalid person UUID due
        # to not finding the UUID in the empty `cpr_cache`.
        payload = importer.generate_manager_payload(instance)
        assert payload["person"]["uuid"] == "None"

    def test_cache_cpr(self):
        employee = {"cpr_no": "0101012222", "uuid": uuid4()}
        importer = los_leder.ManagerImporter()
        with self._mock_lookup_employees(return_value=[employee]):
            importer.cache_cpr()
            assert importer.cpr_cache == {employee["cpr_no"]: employee["uuid"]}

    def test_run(self):
        mock_last_import = datetime(2020, 1, 1)
        mock_filenames = [
            "Leder_nye_20200101_000001.csv",
            "Leder_ret_20200101_000001.csv",
            "Leder_luk_20200101_000001.csv",
        ]
        expected_filedate = datetime(2020, 1, 1, 0, 0, 1)
        with self._mock_get_import_filenames(
            mock_filenames, datetime(2020, 1, 1, 0, 0, 1)
        ) as mock_get_import_filenames:
            importer = _TestableManagerImporter()
            # Run method under test
            self._run_until_complete(importer.run(mock_last_import))
            # Assert `get_import_filenames` was called (without arguments)
            mock_get_import_filenames.assert_called_with()
            # Assert that `handle_{create,edit,terminate}` were called with the
            # expected arguments.
            methods = ("handle_create", "handle_edit", "handle_terminate")
            for method, filename in zip(methods, mock_filenames):
                expected_call = (method, filename, expected_filedate)
                assert expected_call in importer._handler_calls

    def _setup(self, instance):
        importer = los_leder.ManagerImporter()
        importer.cpr_cache = {instance.cpr: str(self._employee_uuid)}
        mock_csv = self._mock_read_csv(instance)
        mock_session = self._mock_get_client_session()
        return importer, mock_csv, mock_session

    def _assert_payload_contains(self, call, expected):
        call.assert_called_once()
        # Second arg of first (and only) call contains the payloads
        payloads = list(call.call_args[0][1])
        assert len(payloads) == 1
        # Assert content and structure of first (and only) payload
        for key, value in expected.items():
            assert payloads[0][key] == value

    def _assert_payload_empty(self, call):
        call.assert_called_once()
        payloads = list(call.call_args[0][1])
        assert len(payloads) == 0


class TestManagerImporterCacheCpr(HelperMixin):
    @settings(max_examples=1000, deadline=None)
    @given(
        # Build simulated return value of `util.lookup_employees`
        st.lists(
            # Employees with both UUID and CPR
            st.fixed_dictionaries({"uuid": st.text(), "cpr_no": st.text()})
            |
            # Employees with only UUID
            st.fixed_dictionaries({"uuid": st.text()})
        )
    )
    def test_cache_cpr_handles_missing_cpr(self, mock_employee_list):
        instance = los_leder.ManagerImporter()
        with self._mock_lookup_employees(return_value=mock_employee_list):
            instance.cache_cpr()
            unique_mock_cprs = set(
                emp["cpr_no"] for emp in mock_employee_list if emp.get("cpr_no")
            )
            assert set(instance.cpr_cache.keys()) == unique_mock_cprs
