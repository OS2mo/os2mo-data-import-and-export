import logging
from uuid import UUID

from fastramqpi.ra_utils.job_settings import JobSettings
from pydantic import AnyHttpUrl
from pydantic import BaseModel
from pydantic import BaseSettings
from pydantic import SecretStr

LOG_LEVEL = logging.DEBUG

logger = logging.getLogger("plan2learn_settings")


class Plan2LearnFTPES(BaseModel):
    hostname: str
    port: int = 21
    username: str
    password: SecretStr


class Settings(BaseSettings):
    auth_server: AnyHttpUrl
    client_id: str
    client_secret: str
    mora_base: str
    auth_realm: str = "mo"

    exporters_plan2learn_root_unit: UUID
    plan2learn_phone_priority: list[UUID] = []
    plan2learn_email_priority: list[UUID] = []
    exporters_plan2learn_allowed_engagement_types: list[UUID] = []
    integrations_SD_Lon_import_too_deep: list[str] = []

    plan2learn_ftpes: Plan2LearnFTPES | None = None

    class Config:
        env_nested_delimiter = "__"


def get_unified_settings(kubernetes_environment: bool) -> Settings:
    # We will not attempt to do either of the following (in order to try not to break
    # things and decrease the code analyzability):
    # 1) use JobSettings in a Kubernetes environment
    # 2) change the on-prem JSON settings
    if kubernetes_environment:
        # read settings from enviromnent variables
        return Settings()  # type: ignore

    job_settings = JobSettings()
    try:
        ftp_settings = Plan2LearnFTPES(
            hostname=job_settings.exporters_plan2learn_host,  # type: ignore
            username=job_settings.exporters_plan2learn_user,  # type: ignore
            password=job_settings.exporters_plan2learn_password,  # type: ignore
        )
    except AttributeError:
        logger.info("FTP-settings not found")
        ftp_settings = None
    return Settings(
        mora_base=job_settings.mora_base,
        auth_server=job_settings.crontab_AUTH_SERVER,  # type: ignore
        client_id=job_settings.crontab_CLIENT_ID,  # type: ignore
        client_secret=job_settings.crontab_CLIENT_SECRET,  # type: ignore
        exporters_plan2learn_allowed_engagement_types=job_settings.exporters_plan2learn_allowed_engagement_types,  # type: ignore
        plan2learn_email_priority=job_settings.plan2learn_email_priority,  # type: ignore
        plan2learn_phone_priority=job_settings.plan2learn_phone_priority,  # type: ignore
        exporters_plan2learn_root_unit=job_settings.exporters_plan2learn_root_unit,  # type: ignore
        integrations_SD_Lon_import_too_deep=job_settings.integrations_SD_Lon_import_too_deep,  # type: ignore
        plan2learn_ftpes=ftp_settings,
    )
