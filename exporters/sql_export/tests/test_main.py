from uuid import uuid4

import pytest

from ..main import handle_class
from ..main import handle_person
from ..sql_table_defs import Bruger
from ..sql_table_defs import Klasse
from ..tests.test_sql_export import _TestableSqlExport


@pytest.mark.asyncio
async def test_handle_person():
    # Arrange
    uuid = uuid4()
    user_dict = {
        "cpr": "2709422807",
        "efternavn": "Lauritzen",
        "fornavn": "Grejs Rajah",
        "from_date": "1942-09-27",
        "kaldenavn": "",
        "kaldenavn_efternavn": "",
        "kaldenavn_fornavn": "",
        "navn": "Grejs Rajah Lauritzen",
        "to_date": "9999-12-31",
        "user_key": "GrejsL",
        "uuid": str(uuid),
    }

    lc_data = {"users": {str(uuid): {str(uuid): [user_dict]}}}
    sql_export = _TestableSqlExport(inject_lc=lc_data)
    user_model = sql_export._generate_sql_users(
        uuid=str(uuid), user_info=user_dict, model=Bruger
    )

    # Act
    await handle_person(uuid=uuid, sql_exporter=sql_export)

    # Assert
    sql_export.session.add.assert_called_once()
    sql_export.session.delete.assert_not_called()
    assert sql_export.session.add.call_args[0][0] == user_model


@pytest.mark.asyncio
async def test_handle_class():
    # Arrange
    uuid = uuid4()
    facet_uuid = uuid4()
    klasse_dict = {
        "user_key": "klasse",
        "title": "test klasse",
        "uuid": str(uuid),
        "facet": str(facet_uuid),
    }

    lc_data = {
        "classes": {str(uuid): {str(uuid): klasse_dict}},
        "facets": {str(facet_uuid): {"user_key": "test_facet"}},
    }
    sql_export = _TestableSqlExport(inject_lc=lc_data)
    class_model = sql_export._generate_sql_classes(
        uuid=str(uuid), klasse_info=klasse_dict, model=Klasse
    )

    # Act
    await handle_class(uuid=uuid, sql_exporter=sql_export)

    # Assert
    sql_export.session.add.assert_called_once()
    sql_export.session.delete.assert_not_called()
    assert sql_export.session.add.call_args[0][0] == class_model
