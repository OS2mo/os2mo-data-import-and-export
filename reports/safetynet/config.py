from enum import Enum
from uuid import UUID

from pydantic import AnyHttpUrl
from pydantic import BaseModel
from pydantic import BaseSettings
from pydantic import SecretStr


class SourceSystem(Enum):
    OPUS = "opus"
    SD = "sd"


class SafetyNetSFTP(BaseModel):
    hostname: str
    port: int
    username: str
    password: SecretStr


class SafetyNetSettings(BaseSettings):
    auth_server: AnyHttpUrl
    client_id: str
    client_secret: SecretStr
    mora_base: str

    safetynet_sftp: SafetyNetSFTP | None = None

    safetynet_adm_unit_uuid: UUID
    safetynet_med_unit_uuid: UUID | None = None

    source_system: SourceSystem = SourceSystem.OPUS

    include_manager_cpr: bool = False
    # Only include these SD engagement types (user keys)
    allowed_sd_engagement_types: list[str] = ["månedsløn"]

    class Config:
        env_nested_delimiter = "__"


def get_settings(*args, **kwargs) -> SafetyNetSettings:
    return SafetyNetSettings(*args, **kwargs)
