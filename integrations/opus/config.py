from pathlib import Path

from fastramqpi.ra_utils.job_settings import JobSettings
from pydantic import AnyHttpUrl
from pydantic import Field
from pydantic import SecretStr


class MOSettings(JobSettings):
    """The inheritance from JobSettings ensures we can read settings from the settings.json file"""

    class Config:
        settings_json_prefix = "crontab"

    auth_realm: str = "mo"
    auth_server: AnyHttpUrl = "http://localhost:5000"
    client_id: str = "opus"
    client_secret: SecretStr
    mo_url: AnyHttpUrl = "http://localhost:5000"


class Settings(JobSettings):
    """The inheritance from JobSettings ensures we can read settings from the settings.json file"""

    mo: MOSettings = Field(default_factory=MOSettings)
    integrations_opus_import_run_db: Path
    integrations_opus_skip_employee_address = False
    integrations_opus_skip_employee_email = False
    integrations_opus_skip_employee_phone = False
    integrations_opus_unit_user_key: str = "@id"
    integrations_opus_units_filter_ids: list[int] = []

    integrations_ad: dict | None = None
