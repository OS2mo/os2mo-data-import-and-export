import logging
import urllib.parse
from enum import Enum
from functools import partial
from typing import Any
from typing import Dict
from typing import Optional
from typing import Tuple

from ra_utils.load_settings import load_settings


logger = logging.getLogger("SqlExport")


class DatabaseFunction(Enum):
    ACTUAL_STATE = 1
    ACTUAL_STATE_HISTORIC = 2


# Mapping from DatabaseFunction + conceptual key to actual keys in settings.json
SETTINGS_MAP = {
    DatabaseFunction.ACTUAL_STATE: {
        "type": "exporters.actual_state.type",
        "host": "exporters.actual_state.host",
        "db_name": "exporters.actual_state.db_name",
        "username": "exporters.actual_state.user",
        "password": "exporters.actual_state.password",
    },
    DatabaseFunction.ACTUAL_STATE_HISTORIC: {
        "type": "exporters.actual_state_historic.type",
        "host": "exporters.actual_state_historic.host",
        "db_name": "exporters.actual_state_historic.db_name",
        "username": "exporters.actual_state_historic.user",
        "password": "exporters.actual_state_historic.password",
    },
}

# Mapping from DatabaseFunction + conceptual key to fallback keys in settings.json
# These are used if the settings map above does not yield a value
FALLBACK_MAP = {
    DatabaseFunction.ACTUAL_STATE_HISTORIC: {
        "host": "exporters.actual_state.host",
        "username": "exporters.actual_state.user",
        "password": "exporters.actual_state.password",
    },
}


class ConfigurationError(Exception):
    """Throw whenever a configuration issue in settings.json is detected."""

    pass


def convert_key_to_setting(key: str, database_function: DatabaseFunction) -> str:
    """Convert a connection key to an actual setting in settings.json."""
    return SETTINGS_MAP[database_function][key]


def convert_key_to_fallback(
    key: str, database_function: DatabaseFunction
) -> Optional[str]:
    """Convert a connection key to a fallback setting in settings.json."""
    return FALLBACK_MAP.get(database_function, {}).get(key)


def load_setting(
    key: str,
    database_function: DatabaseFunction,
    settings: Dict,
) -> Optional[str]:
    setting = convert_key_to_setting(key, database_function)
    fallback_setting = convert_key_to_fallback(key, database_function)

    value = settings.get(setting)
    if value is None and fallback_setting:
        logger.warning(f"Utilizing {fallback_setting} in place of {setting}!")
        value = settings.get(fallback_setting)
    return value


def get_db_type(
    database_function: DatabaseFunction, force_sqlite: bool, settings: Dict
) -> str:
    if force_sqlite:
        return "SQLite"
    value = load_setting("type", database_function, settings)
    if value is None:
        raise ConfigurationError("Missing value in db_type")
    return value


def get_db_name(database_function: DatabaseFunction, settings: Dict) -> str:
    value = load_setting("db_name", database_function, settings)
    if value is None:
        raise ConfigurationError("Missing value in db_name")
    return value


get_db_host = partial(load_setting, "host")
get_db_username = partial(load_setting, "username")
get_db_password = partial(load_setting, "password")


def generate_db_type_and_name(
    database_function: DatabaseFunction, force_sqlite: bool, settings: Dict
) -> Tuple[str, str]:
    return (
        get_db_type(database_function, force_sqlite, settings),
        get_db_name(database_function, settings),
    )


def generate_connection_url(
    database_function: DatabaseFunction,
    force_sqlite: bool = False,
    settings: Optional[Dict] = None,
) -> str:
    """Utilize settings or settings from disk to derive database connection url."""
    settings = settings or load_settings()

    db_type, db_name = generate_db_type_and_name(
        database_function, force_sqlite, settings
    )
    user = get_db_username(database_function, settings)
    db_host = get_db_host(database_function, settings)
    pw_raw = get_db_password(database_function, settings)
    pw_raw = pw_raw or ""
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
    database_function: DatabaseFunction,
    force_sqlite: bool = False,
    settings: Optional[Dict] = None,
) -> Dict[str, Any]:
    settings = settings or load_settings()

    db_type = get_db_type(database_function, force_sqlite, settings)
    engine_settings: Dict = {"pool_pre_ping": True}
    if db_type == "Mysql":
        engine_settings.update({"pool_recycle": 3600})
    return engine_settings
