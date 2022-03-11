from typing import Any
from typing import Dict
from typing import Optional
from uuid import UUID

import uuids
from pydantic import BaseSettings
from ra_utils.load_settings import load_settings


def json_config_settings_source(settings: BaseSettings) -> Dict[str, Any]:
    settings_json = load_settings() or {}
    prefix = "integrations.aarhus_los"

    def _get_setting_value(key: str, default: Any = None):
        return settings_json.get(f"{prefix}.{key}", default)

    return dict(
        ftp_url=_get_setting_value("ftp_url", "ftp.aarhuskommune.dk"),
        ftp_user=_get_setting_value("ftp_user"),
        ftp_pass=_get_setting_value("ftp_pass"),
        ftp_folder=_get_setting_value("ftp_folder", "TEST"),
        import_state_file=_get_setting_value("state_file"),
        import_csv_folder=_get_setting_value("import_csv_folder"),
        azid_it_system_uuid=_get_setting_value(
            "azid_it_system_uuid", uuids.AZID_SYSTEM
        ),
    )


class ImproperlyConfigured(Exception):
    pass


class Settings(BaseSettings):
    ftp_url: str
    ftp_user: str
    ftp_pass: str
    ftp_folder: str
    import_state_file: str
    import_csv_folder: Optional[str]
    mox_base: str = "http://localhost:8080"
    mora_base: str = "http://localhost:5000"
    queries_dir: str = "/opt/docker/os2mo/queries"
    max_concurrent_requests: int = 4
    os2mo_chunk_size: int = 20
    azid_it_system_uuid: UUID

    class Config:
        @classmethod
        def customise_sources(
            cls,
            init_settings,
            env_settings,
            file_secret_settings,
        ):
            return (
                init_settings,
                env_settings,
                json_config_settings_source,
                file_secret_settings,
            )

    @classmethod
    def from_kwargs(cls, **kwargs):
        """Return a `Settings` instance populated based on `kwargs`.

        This is usually called from a function decorated with `@click.command`:

            @click.command()
            def main(**kwargs):
                settings = Settings.from_kwargs(**kwargs)
        """
        instance = cls(**kwargs)
        # Save populated `Settings` instance so `get_config` can retrieve it
        cls._instance = instance
        return instance


def get_config() -> Settings:
    try:
        return Settings._instance  # type: ignore
    except AttributeError:
        raise ImproperlyConfigured(
            "Settings are not configured - did you call `Settings.from_kwargs`?"
        )
