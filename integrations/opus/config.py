from pathlib import Path

from fastramqpi.ra_utils.job_settings import JobSettings
from pydantic import AnyHttpUrl
from pydantic import BaseSettings
from pydantic import Field
from pydantic import SecretStr


class MOSettings(BaseSettings):
    class Config:
        settings_json_prefix = "crontab"

    auth_realm: str = "mo"
    auth_server: AnyHttpUrl = "http://localhost:5000"
    client_id: str = "opus"
    client_secret: SecretStr
    mo_url: AnyHttpUrl = "http://localhost:5000"


class Settings(BaseSettings):
    mo: MOSettings = Field(default_factory=MOSettings)
    integrations_opus_import_run_db: Path
    integrations_opus_skip_employee_address = False
    integrations_opus_skip_employee_email = False
    integrations_opus_skip_employee_phone = False
    integrations_opus_unit_user_key: str = "@id"
    integrations_opus_units_filter_ids: list[int] = []

    integrations_ad: dict | None = None


class OpusSettings(Settings, JobSettings):
    class Config:
        frozen = True
        env_nested_delimiter = "__"

    """The inheritance from JobSettings ensures we can read settings from the settings.json file.
    Remove once opus runs in docker and has all variables in environment variables."""

    pass
