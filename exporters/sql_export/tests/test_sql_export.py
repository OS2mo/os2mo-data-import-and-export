import pytest
from sqlalchemy import inspect

from exporters.sql_export.sql_export import SqlExport


class FakeLC:
    """Fake version of LoraCache, presenting the empty member dicts.

    LoraCache's interface is essentially just these dictionaries.
    """

    classes = {}
    addresses = {}
    facets = {}
    users = {}
    units = {}
    dar_cache = {}
    engagements = {}
    associations = {}
    roles = {}
    leaves = {}
    managers = {}
    itsystems = {}
    it_connections = {}
    kles = {}
    related = {}

    def calculate_primary_engagements(self):
        raise NotImplementedError()

    def calculate_derived_unit_data(self):
        raise NotImplementedError()

    def populate_cache(self, dry_run=False, skip_associations=False):
        raise NotImplementedError()


class FakeLCSqlExport(SqlExport):
    def _get_lora_cache(self, resolve_dar, use_pickle):
        return FakeLC()


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
