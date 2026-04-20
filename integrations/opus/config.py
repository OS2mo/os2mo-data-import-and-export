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
