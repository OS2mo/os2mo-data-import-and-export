from fastramqpi.ra_utils.job_settings import JobSettings


class ClearAndImportOpusSettings(JobSettings):
    class Config(JobSettings.Config):
        fronzen = True

    mox_base: str = "http://localhost:5000/lora"
    integrations_opus_units_filter_ids: list[str] = []
    integrations_opus_import_run_db: str | None = None


class OpusCheckDarAddressesSettings(JobSettings):
    class Config(JobSettings.Config):
        frozen = True

    integrations_opus_units_filter_ids: list[str] = []


class OpusDiffImportSettings(JobSettings):
    class Config(JobSettings.Config):
        frozen = True

    integrations_opus_units_filter_ids: list[str] = []
    integrations_opus_skip_employee_email: bool = False
    integrations_opus_skip_employee_phone: bool = False
    integrations_opus_skip_employee_address: bool = False
    integrations_opus_unit_user_key: str | None = None
    integrations_opus_import_run_db: str | None = None
    integrations_ad: bool = False
