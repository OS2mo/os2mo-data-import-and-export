import json
import logging
import os
from pydantic import BaseSettings
from typing import List, Dict, Any, Optional
# flake8: noqa
from ra_utils.load_settings import load_settings

def _relpath(filename):
    return os.path.join(os.getcwd(), filename)


def json_config_settings_source(settings: BaseSettings) -> Dict[str, Any]:
    """
    A simple settings source that loads variables from a JSON file
    at the project's root.

    Here we happen to choose to use the `env_file_encoding` from Config
    when reading `config.json`
    """
    all_settings = load_settings()
    os2sync_settings = dict(filter(apply(
        lambda key, value: key.startswith('os2sync')),
        all_settings.items()
    ))
    final_settings = {key.lstrip("os2sync"): val for key, val in os2sync_settings.items()}
    final_settings.update({"OS2MO_SAML_TOKEN":all_settings.get('crontab.SAML_TOKEN')})
    return final_settings


class Settings(BaseSettings):

    os2sync_ca_bundle: Optional[str]

    OS2MO_TOP_UNIT_UUID: Optional[str]
    OS2SYNC_CA_BUNDLE: Optional[str]
    OS2SYNC_MUNICIPALITY: Optional[str]
    MOX_LOG_LEVEL: Optional[str]
    MOX_LOG_FILE: Optional[str]
    OS2MO_SERVICE_URL: Optional[str]
    OS2MO_SAML_TOKEN: Optional[str]
    OS2SYNC_HASH_CACHE: Optional[str]
    OS2SYNC_API_URL: Optional[str]
    OS2SYNC_XFER_CPR: Optional[str]
    OS2SYNC_USE_LC_DB: Optional[str]
    OS2SYNC_PHONE_SCOPE_CLASSES: Optional[List[str]]
    OS2SYNC_EMAIL_SCOPE_CLASSES: Optional[List[str]]
    OS2SYNC_IGNORED_UNIT_LEVELS: Optional[List[str]]
    OS2SYNC_IGNORED_UNIT_TYPES: Optional[List[str]]
    OS2SYNC_AUTOWASH: bool = False
    OS2SYNC_TEMPLATES: Dict = {}
    OS2MO_HAS_KLE: bool = True
    OS2MO_ORG_UUID: str = ""

    class Config:
        env_file_encoding = 'utf-8'
        extra='ignore'

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
logformat = '%(levelname)s %(asctime)s %(name)s %(message)s'
loggername = "os2sync"
