from fastramqpi.ra_utils.job_settings import JobSettings


class RollekatalogSettings(JobSettings):
    class Config:
        settings_json_prefix = "exporters.os2rollekatalog"
