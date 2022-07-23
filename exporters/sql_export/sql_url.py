import logging
import urllib.parse
from typing import Any
from typing import Dict

from .config import DatabaseFunction
from .config import Settings


logger = logging.getLogger("SqlExport")


def generate_connection_url(
    database_function: DatabaseFunction,
    force_sqlite: bool,
    settings: Settings,
) -> str:
    """Utilize settings or settings from disk to derive database connection url."""
    db_type = settings.get_db_type(database_function, force_sqlite)
    db_name = settings.get_db_name(database_function)
    user = settings.get_db_username(database_function)
    db_host = settings.get_db_host(database_function)
    pw_raw = settings.get_db_password(database_function)
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
    force_sqlite: bool,
    settings: Settings,
) -> Dict[str, Any]:
    db_type = settings.get_db_type(database_function, force_sqlite)

    engine_settings: Dict = {"pool_pre_ping": True}
    if db_type == "Mysql":
        engine_settings["pool_recycle"] = 3600
    return engine_settings
