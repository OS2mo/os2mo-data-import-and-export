from uuid import UUID

from ra_utils.job_settings import JobSettings


class RollekatalogSettings(JobSettings):
    exporters_os2rollekatalog_rollekatalog_url: str | None
    exporters_os2rollekatalog_rollekatalog_api_key: UUID | None
    exporters_os2rollekatalog_main_root_org_unit: UUID
    exporters_os2rollekatalog_ou_filter: bool = False
    exporters_os2rollekatalog_rollekatalog_root_uuid: UUID | None
    exporters_os2rollekatalog_mapping_file_path: str = "cpr_mo_ad_map.csv"
    exporters_os2rollekatalog_use_nickname: bool = False
    exporters_os2rollekatalog_sync_titles: bool = False

    class Config:
        settings_json_prefix = "exporters.os2rollekatalog"
