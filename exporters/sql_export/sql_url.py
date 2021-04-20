import urllib.parse
from enum import Enum
from typing import Any, Dict, Tuple

from exporters.utils.load_settings import load_settings


class DatabaseFunction(Enum):
    ACTUAL_STATE = 1
    ACTUAL_STATE_HISTORIC = 2


def generate_db_type_and_name(
    database_function: DatabaseFunction, force_sqlite: bool, settings: Dict
) -> Tuple[str, str]:
    keymap = {
        DatabaseFunction.ACTUAL_STATE: (
            "exporters.actual_state.type",
            "exporters.actual_state.db_name",
        ),
        DatabaseFunction.ACTUAL_STATE_HISTORIC: (
            "exporters.actual_state_historic.type",
            "exporters.actual_state_historic.db_name",
        ),
    }
    db_type_key, db_name_key = keymap[database_function]
    db_type = settings.get(db_type_key)
    db_name = settings.get(db_name_key)

    if force_sqlite:
        db_type = "SQLite"

    if None in [db_type, db_name]:
        msg = "Configuration error, missing db name or type"
        raise Exception(msg)

    return str(db_type), str(db_name)


def generate_connection_url(
    database_function: DatabaseFunction, force_sqlite: bool = False, settings=None
) -> str:
    """Utilize settings or settings from disk to derive database connection url."""
    settings = settings or load_settings()

    db_type, db_name = generate_db_type_and_name(
        database_function, force_sqlite, settings
    )
    user = settings.get("exporters.actual_state.user")
    db_host = settings.get("exporters.actual_state.host")
    pw_raw = settings.get("exporters.actual_state.password", "")
    pw = urllib.parse.quote_plus(pw_raw)

    if db_type == "Memory":
        return "sqlite://"
    if db_type == "SQLite":
        return "sqlite:///{}.db".format(db_name)
    if db_type == "MS-SQL":
        return "mssql+pymssql://{}:{}@{}/{}".format(user, pw, db_host, db_name)
    if db_type == "MS-SQL-ODBC":
        quoted = urllib.parse.quote_plus(
            (
                "DRIVER=libtdsodbc.so;Server={};Database={};UID={};"
                + "PWD={};TDS_Version=8.0;Port=1433;"
            ).format(db_host, db_name, user, pw_raw)
        )
        return "mssql+pyodbc:///?odbc_connect={}".format(quoted)
    if db_type == "Mysql":
        return "mysql+mysqldb://{}:{}@{}/{}".format(user, pw, db_host, db_name)
    if db_type == "Postgres":
        return "postgresql://{}:{}@{}/{}".format(user, pw, db_host, db_name)
    raise Exception("Unknown DB type")


def generate_engine_settings(
    database_function: DatabaseFunction, force_sqlite: bool = False, settings=None
) -> Dict[str, Any]:
    settings = settings or load_settings()

    db_type, _ = generate_db_type_and_name(database_function, force_sqlite, settings)
    engine_settings: Dict = {"pool_pre_ping": True}
    if db_type == "Mysql":
        engine_settings.update({"pool_recycle": 3600})
    return engine_settings
