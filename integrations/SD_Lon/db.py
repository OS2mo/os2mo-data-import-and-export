import logging
from functools import lru_cache
from typing import Tuple

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from integrations.SD_Lon.config import get_changed_at_settings


logger = logging.getLogger("sdChangedAt")


@lru_cache()
def _get_db_engine():
    settings = get_changed_at_settings()
    if settings.sd_import_run_db is not None:
        url = f"sqlite:///{settings.sd_import_run_db}"
        return create_engine(url)

    url = f"postgresql://{settings.sd_user}:{settings.sd_password}" \
          f"@{settings.sd_run_db_host}:{settings.sd_run_db_port}/{settings.sd_run_db_name}"
    return create_engine(url)


def read_last_line() -> Tuple[str, str, str]:
    engine = _get_db_engine()
    stmt = "SELECT * FROM runs ORDER BY id DESC LIMIT 1"
    try:
        with engine.begin() as conn:
            cursor = conn.execute(text(stmt))
            row = cursor.fetchone()
            return row.from_date, row.to_date, row.status
    except SQLAlchemyError as err:
        logger.error("Problem communicating with the DB", err=err)


if __name__ == '__main__':
    engine = create_engine("sqlite:////opt/dipex/run_db.sqlite")
    x, y, z = read_last_line(engine)
    print(x, y, z)
