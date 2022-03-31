from functools import lru_cache
from typing import Optional

from sqlalchemy import create_engine

from integrations.SD_Lon.config import ChangedAtSettings


@lru_cache()
def get_db_engine(settings: ChangedAtSettings):
    if settings.sd_import_run_db is not None:
        url = f"sqlite://{settings.sd_import_run_db}"
        return create_engine(url)

    url = f"postgresql://{settings.sd_user}:{settings.sd_password}" \
          f"@{settings.sd_run_db_host}:{settings.sd_run_db_port}/{settings.sd_run_db_name}"
    return create_engine(url)
