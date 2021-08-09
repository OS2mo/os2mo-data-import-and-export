import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseSettings
from ra_utils.apply import apply
from ra_utils.headers import TokenSettings

# flake8: noqa
from ra_utils.load_settings import load_settings


def _relpath(filename):
    return os.path.join(os.getcwd(), filename)


def json_config_settings_source(settings: BaseSettings) -> Dict[str, Any]:
    """
    Read config from settings.json.

    Reads all keys starting with 'os2sync.' and a few common settings into Settings.
    """
    all_settings = load_settings()
    #Read os2sync specific settings
    os2sync_settings = dict(
        filter(
            apply(lambda key, value: key.startswith("os2sync")), all_settings.items()
        )
    )
    # Trim leading 'os2sync.'
    trimlen = len("os2sync.")
    final_settings = {key[trimlen:]: val for key, val in os2sync_settings.items()}
    #Add needed common settings
    final_settings.update(
        {
            "OS2SYNC_MUNICIPALITY": all_settings.get("municipality.cvr"),
            "OS2MO_SERVICE_URL": all_settings.get("mora.base", "http://localhost:5000")
            + "/service",
        }
    )

    return final_settings


class Settings(BaseSettings):

    #common:
    OS2SYNC_MUNICIPALITY: str   #Called "municipality.cvr" in settings.json
    OS2MO_SERVICE_URL: str      #"mora.base" from settings.json + /service
    
    #os2sync:
    top_unit_uuid: str
    os2sync_ca_bundle: str
    os2sync_api_url: str
    
    log_level: int = logging.INFO
    log_file: Path = _relpath("../os2sync.log")
    hash_cache: Path = _relpath("../os2sync_hash_cache")
    xfer_cpr: bool = False
    use_lc_db: bool = False
    autowash: bool = False
    
    phone_scope_classes: List[str] = []
    email_scope_classes: List[str] = []
    ignored_unit_levels: List[str] = []
    ignored_unit_types: List[str] = []
    templates: Dict = {}

    # dont set - probed unless set:
    OS2MO_ORG_UUID: str = ""   
    OS2MO_HAS_KLE: bool = False  
    OS2SYNC_TRUNCATE: int = 200 


    class Config:
        env_file_encoding = "utf-8"
        extra = "ignore"

        @classmethod
        def customise_sources(
            cls,
            init_settings,
            env_settings,
            file_secret_settings,
        ):
            return (
                init_settings,
                env_settings,
                json_config_settings_source,
                file_secret_settings,
            )


settings = Settings().dict()
logformat = "%(levelname)s %(asctime)s %(name)s %(message)s"
loggername = "os2sync"
