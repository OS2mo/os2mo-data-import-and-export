from pathlib import Path

from fastramqpi.ra_utils.job_settings import JobSettings


class OpusSettings(JobSettings):
    class Config(JobSettings.Config):
        frozen = True

    integrations_opus_units_filter_ids: list[str] = []
    integrations_opus_import_run_db: str


class ClearAndImportOpusSettings(OpusSettings):
    # this calss exists for consistency, even if it doesn't define any fields
    pass


class OpusCheckDarAddressesSettings(OpusSettings):
    # this class exists for consistency, even if it doesn't define any fields
    pass


class OpusDiffImportSettings(OpusSettings):
    integrations_opus_skip_employee_email: bool = False
    integrations_opus_skip_employee_phone: bool = False
    integrations_opus_skip_employee_address: bool = False
    integrations_opus_unit_user_key: str | None = None
    integrations_ad: bool = False


class OpusFileReaderSettings(OpusSettings):
    integrations_opus_gcloud_bucket_name: str | None = None
    integrations_opus_smb_user: str
    integrations_opus_smb_password: str
    integrations_opus_smb_host: str | None = None
    integrations_opus_import_xml_path: Path


class OpusHelpersSettings(OpusSettings):
    municipality_name: str
