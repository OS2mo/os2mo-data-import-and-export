from uuid import UUID

from pydantic import AnyHttpUrl
from pydantic import BaseSettings
from pydantic import SecretStr


class SafetyNetSettings(BaseSettings):
    auth_server: AnyHttpUrl
    client_id: str
    client_secret: SecretStr
    mora_base: str

    safetynet_sftp_hostname: str
    safetynet_sftp_port: str
    safetynet_sftp_username: str
    safetynet_sftp_password: SecretStr

    safetynet_adm_unit_uuid: UUID
    safetynet_med_unit_uuid: UUID


def get_settings(*args, **kwargs) -> SafetyNetSettings:
    return SafetyNetSettings(*args, **kwargs)
