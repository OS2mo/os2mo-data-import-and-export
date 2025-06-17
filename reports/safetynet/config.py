from uuid import UUID

from pydantic import AnyHttpUrl
from pydantic import BaseSettings
from pydantic import SecretStr


class SafetyNetSettings(BaseSettings):
    auth_server: AnyHttpUrl
    client_id: str
    client_secret: SecretStr
    mora_base: str

    safetynet_sftp_hostname: str | None = None
    safetynet_sftp_port: int | None = None
    safetynet_sftp_username: str | None = None
    safetynet_sftp_password: SecretStr | None = None

    safetynet_adm_unit_uuid: UUID
    safetynet_med_unit_uuid: UUID | None = None


def get_settings(*args, **kwargs) -> SafetyNetSettings:
    return SafetyNetSettings(*args, **kwargs)
