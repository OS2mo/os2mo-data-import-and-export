from uuid import uuid4
from unittest.mock import MagicMock
from ..sql_export import SqlExport
from ..sql_table_defs import WItForbindelse, WItForbindelseEngagement, WAdresse, Base
from typing import Any, Dict
from more_itertools import one

def _mk_uuid() -> str:
    return str(uuid4())

class FakeLC:
    classes: Dict[str, Any] = {}
    addresses: Dict[str, Any] = {}
    facets: Dict[str, Any] = {}
    users: Dict[str, Any] = {}
    units: Dict[str, Any] = {}
    dar_cache: Dict[str, Any] = {}
    engagements: Dict[str, Any] = {}
    associations: Dict[str, Any] = {}
    leaves: Dict[str, Any] = {}
    managers: Dict[str, Any] = {}
    itsystems: Dict[str, Any] = {}
    it_connections: Dict[str, Any] = {}
    kles: Dict[str, Any] = {}
    related: Dict[str, Any] = {}

    def calculate_primary_engagements(self):
        pass

    def calculate_derived_unit_data(self):
        pass

    def populate_cache(self, dry_run=False, skip_associations=False):
        pass

class _TestableSqlExport(SqlExport):
    def __init__(self, inject_lc=None):
        super().__init__(force_sqlite=False, historic=False, settings={})
        self.inject_lc = inject_lc
        self.lc = self._get_lora_cache(resolve_dar=False, use_pickle=True)
        self.session = MagicMock()

    def _get_engine(self):
        return MagicMock()

    def _get_db_session(self):
        return self.session

    def _get_export_cpr_setting(self) -> bool:
        return True

    def _get_lora_cache(self, resolve_dar, use_pickle):
        lc = FakeLC()
        if self.inject_lc:
            for key, values in self.inject_lc.items():
                setattr(lc, key, values)
        return lc

    def _get_lora_class(self, uuid: str):
         return uuid, {"title": "Title", "user_key": "UserKey", "scope": "Scope"}

def _assert_db_session_add(session: MagicMock, cls: Base, **expected: Any) -> None:
    session_add_calls = [
        call
        for call in session.method_calls
        if len(call.args) > 0 and isinstance(one(call.args), cls)
    ]
    assert len(session_add_calls) >= 1, f"No calls for {cls}"
    found = False
    for call in session_add_calls:
        model = one(call.args)
        match = True
        for name, value in expected.items():
            if getattr(model, name) != value:
                match = False
                break
        if match:
            found = True
            break
    assert found, f"Expected {expected} in calls for {cls}"

def test_it_connection_extensions():
    it_user_uuid = _mk_uuid()
    engagement_uuid_1 = _mk_uuid()
    engagement_uuid_2 = _mk_uuid()
    external_id = "ext-123"
    
    it_user = {
        "uuid": it_user_uuid,
        "user": _mk_uuid(),
        "unit": _mk_uuid(),
        "username": "username",
        "itsystem": _mk_uuid(),
        "primary_boolean": True,
        "external_id": external_id,
        "engagement_uuids": [engagement_uuid_1, engagement_uuid_2],
        "from_date": "2020-01-01",
        "to_date": "2020-01-01",
    }
    
    lc_data = {"it_connections": {it_user_uuid: [it_user]}}
    sql_export = _TestableSqlExport(inject_lc=lc_data)
    
    sql_export.perform_export()
    
    _assert_db_session_add(
        sql_export.session,
        WItForbindelse,
        uuid=it_user_uuid,
        eksternt_id=external_id
    )
    
    _assert_db_session_add(
        sql_export.session,
        WItForbindelseEngagement,
        it_forbindelse_uuid=it_user_uuid,
        engagement_uuid=engagement_uuid_1,
        startdato="2020-01-01"
    )
    _assert_db_session_add(
        sql_export.session,
        WItForbindelseEngagement,
        it_forbindelse_uuid=it_user_uuid,
        engagement_uuid=engagement_uuid_2
    )

def test_address_extensions():
    address_uuid = _mk_uuid()
    engagement_uuid = _mk_uuid()
    
    address = {
        "uuid": address_uuid,
        "user": _mk_uuid(),
        "unit": _mk_uuid(),
        "user_key": "Beskrivelse",
        "value": "Værdi",
        "scope": "DAR",
        "dar_uuid": _mk_uuid(),
        "adresse_type": _mk_uuid(),
        "visibility": _mk_uuid(),
        "engagement_uuid": engagement_uuid,
        "from_date": "2020-01-01",
        "to_date": "2020-01-01",
    }
    
    facet_uuid = _mk_uuid()
    
    lc_data = {
        "addresses": {address_uuid: [address]},
        "classes": {
            address["adresse_type"]: {"title": "TypeTitle", "user_key": "TypeKey", "scope": "TypeScope", "facet": facet_uuid},
            address["visibility"]: {"title": "VisTitle", "user_key": "VisKey", "scope": "VisScope", "facet": facet_uuid}
        },
        "facets": {
            facet_uuid: {"user_key": "FacetKey"}
        }
    }
    
    sql_export = _TestableSqlExport(inject_lc=lc_data)
    sql_export.perform_export()
    
    _assert_db_session_add(
        sql_export.session,
        WAdresse,
        uuid=address_uuid,
        engagement_uuid=engagement_uuid
    )
