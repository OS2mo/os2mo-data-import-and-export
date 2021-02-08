from functools import lru_cache
from typing import Dict, List, Optional
from uuid import UUID

from pydantic import AnyUrl, BaseSettings, HttpUrl


class Settings(BaseSettings):
    mora_url: HttpUrl = "http://mo:5000"
    saml_token: Optional[str]

    triggered_uuids: List[UUID]
    ou_levelkeys: List[str]
    ou_time_planning_mo_vs_sd: Dict[str, str]

    amqp_username: str
    amqp_password: str
    amqp_host: str = "msg-amqp.silkeborgdata.dk"
    amqp_virtual_host: str
    amqp_port: int = 5672
    amqp_check_waittime: int = 3
    amqp_check_retries: int = 6

    sd_username: str
    sd_password: str
    sd_institution: str
    sd_base_url: str = "https://service.sd.dk/sdws/"


@lru_cache
def get_settings(**overrides):
    settings = Settings(**overrides)
    return settings
