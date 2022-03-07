from typing import Any
from typing import Dict
from unittest.mock import MagicMock
from uuid import uuid4

from alchemy_mock.mocking import UnifiedAlchemyMagicMock
from sqlalchemy import inspect
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from exporters.sql_export.lora_cache import LoraCache
from exporters.sql_export.sql_export import SqlExport
from exporters.sql_export.sql_table_defs import ItForbindelse
from exporters.sql_export.sql_table_defs import Tilknytning


class FakeLC:
    """Fake version of LoraCache, presenting the empty member dicts.

    LoraCache's interface is essentially just these dictionaries.
    """

    classes: Dict[str, Any] = {}
    addresses: Dict[str, Any] = {}
    facets: Dict[str, Any] = {}
    users: Dict[str, Any] = {}
    units: Dict[str, Any] = {}
    dar_cache: Dict[str, Any] = {}
    engagements: Dict[str, Any] = {}
    associations: Dict[str, Any] = {}
    roles: Dict[str, Any] = {}
    leaves: Dict[str, Any] = {}
    managers: Dict[str, Any] = {}
    itsystems: Dict[str, Any] = {}
    it_connections: Dict[str, Any] = {}
    kles: Dict[str, Any] = {}
    related: Dict[str, Any] = {}

    def calculate_primary_engagements(self):
        raise NotImplementedError()

    def calculate_derived_unit_data(self):
        raise NotImplementedError()

    def populate_cache(self, dry_run=False, skip_associations=False):
        raise NotImplementedError()


class FakeLCSqlExport(SqlExport):
    def _get_lora_cache(self, resolve_dar, use_pickle):
        return FakeLC()


class _TestableSqlExport(SqlExport):
    def __init__(self, inject_lc=None):
        super().__init__(force_sqlite=False, historic=False, settings={})
        self.inject_lc = inject_lc

    def _get_engine(self) -> Engine:
        return MagicMock()

    def _get_db_session(self) -> Session:
        return UnifiedAlchemyMagicMock()

    def _get_export_cpr_setting(self) -> bool:
        return True

    def _get_lora_cache(self, resolve_dar, use_pickle) -> LoraCache:
        lc = FakeLC()
        if self.inject_lc:
            for key, values in self.inject_lc.items():
                setattr(lc, key, values)
        return lc


def check_tables(engine, expected):
    inspector = inspect(engine)

    schemas = inspector.get_schema_names()
    assert len(schemas) == 1
    assert schemas == ["main"]
    schema = schemas[0]

    table_names = inspector.get_table_names(schema=schema)
    assert table_names == expected


def test_sql_export_tables():
    settings = {
        "exporters.actual_state.type": "Memory",
        "exporters.actual_state.db_name": "Whatever",
    }
    sql_export = FakeLCSqlExport(
        force_sqlite=False,
        historic=False,
        settings=settings,
    )
    check_tables(sql_export.engine, [])

    sql_export.perform_export(
        resolve_dar=False,
        use_pickle=False,
    )
    check_tables(
        sql_export.engine,
        [
            "kvittering",
            "wadresser",
            "wbrugere",
            "wdar_adresser",
            "wengagementer",
            "wenheder",
            "wenhedssammenkobling",
            "wfacetter",
            "wit_forbindelser",
            "wit_systemer",
            "wklasser",
            "wkle",
            "wleder_ansvar",
            "wledere",
            "worlover",
            "wroller",
            "wtilknytninger",
        ],
    )

    sql_export.swap_tables()
    check_tables(
        sql_export.engine,
        [
            "adresser",
            "brugere",
            "dar_adresser",
            "engagementer",
            "enheder",
            "enhedssammenkobling",
            "facetter",
            "it_forbindelser",
            "it_systemer",
            "klasser",
            "kle",
            "kvittering",
            "leder_ansvar",
            "ledere",
            "orlover",
            "roller",
            "tilknytninger",
        ],
    )


def _mk_uuid():
    return str(uuid4())


def _mock_lora_class(name):
    facet_uuid = _mk_uuid()
    facet = {"user_key": f"{name}_facet"}
    cls_uuid = _mk_uuid()
    cls = {
        "user_key": f"{name}_class",
        "title": f"{name}",
        "scope": "scope",
        "facet": facet_uuid,
    }
    return {cls_uuid: cls}, {facet_uuid: facet}


def _get_cls_uuid(val: dict):
    return list(val.keys())[0]


def _get_cls_field(cls: dict, field: str):
    return list(cls.values())[0][field]


def _join_dicts(*dicts: Dict):
    result = {}
    for val in dicts:
        result.update(val)
    return result


def _assert_db_session_add(session, cls, **expected):
    session_add_calls = [
        call
        for call in session.method_calls
        if len(call.args) > 0 and isinstance(call.args[0], cls)
    ]
    assert len(session_add_calls) == 1
    model = session_add_calls[0].args[0]
    for name, value in expected.items():
        assert getattr(model, name) == value


def test_sql_export_writes_associations():
    # Arrange
    assoc_type_cls, assoc_type_facet = _mock_lora_class("assoc_type")
    job_function_cls, job_function_facet = _mock_lora_class("job_function")
    assoc_uuid = _mk_uuid()
    assoc = {
        "uuid": assoc_uuid,
        "user": _mk_uuid(),
        "unit": _mk_uuid(),
        "user_key": "assoc",
        "association_type": _get_cls_uuid(assoc_type_cls),
        "job_function": _get_cls_uuid(job_function_cls),
        "it_user": _mk_uuid(),
        "from_date": "2020-01-01",
        "to_date": "2020-01-01",
    }
    lc_data = {
        "facets": _join_dicts(assoc_type_facet, job_function_facet),
        "classes": _join_dicts(assoc_type_cls, job_function_cls),
        "associations": {assoc_uuid: [assoc]},
    }
    sql_export = _TestableSqlExport(inject_lc=lc_data)

    # Act
    sql_export.perform_export()

    # Assert
    _assert_db_session_add(
        sql_export.session,
        Tilknytning,
        uuid=assoc_uuid,
        bruger_uuid=assoc["user"],
        enhed_uuid=assoc["unit"],
        bvn=assoc["user_key"],
        tilknytningstype_uuid=_get_cls_uuid(assoc_type_cls),
        tilknytningstype_titel=_get_cls_field(assoc_type_cls, "title"),
        startdato=assoc["from_date"],
        slutdato=assoc["to_date"],
        stillingsbetegnelse_uuid=assoc["job_function"],
        stillingsbetegnelse_titel=_get_cls_field(job_function_cls, "title"),
        it_forbindelse_uuid=assoc["it_user"],
    )


def test_sql_export_writes_it_users():
    # Arrange
    it_user_uuid = _mk_uuid()
    it_user = {
        "uuid": it_user_uuid,
        "user": _mk_uuid(),
        "unit": _mk_uuid(),
        "username": "username",
        "itsystem": _mk_uuid(),
        "primary_type": _mk_uuid(),
        "from_date": "2020-01-01",
        "to_date": "2020-01-01",
    }
    lc_data = {"it_connections": {it_user_uuid: [it_user]}}
    sql_export = _TestableSqlExport(inject_lc=lc_data)

    # Act
    sql_export.perform_export()

    # Assert
    _assert_db_session_add(
        sql_export.session,
        ItForbindelse,
        uuid=it_user_uuid,
        it_system_uuid=it_user["itsystem"],
        bruger_uuid=it_user["user"],
        enhed_uuid=it_user["unit"],
        brugernavn=it_user["username"],
        startdato=it_user["from_date"],
        slutdato=it_user["to_date"],
        primær_boolean=it_user["primary_type"] is not None,
    )
