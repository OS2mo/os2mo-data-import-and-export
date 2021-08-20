from functools import lru_cache

from pydantic import BaseModel
from ra_utils.load_settings import load_settings


class Settings(BaseModel):
    ftp_url: str
    ftp_user: str
    ftp_pass: str
    ftp_folder: str
    import_state_file: str
    mox_base: str = "http://localhost:8080"
    mora_base: str = "http://localhost:5000"
    queries_dir: str = "/opt/docker/os2mo/queries"
    max_concurrent_requests: int = 4
    os2mo_chunk_size: int = 20


@lru_cache()
def get_config() -> Settings:
    top_settings = load_settings()

    return Settings(
        ftp_url=top_settings.get(
            "integrations.aarhus_los.ftp_url", "ftp.aarhuskommune.dk"
        ),
        ftp_user=top_settings["integrations.aarhus_los.ftp_user"],
        ftp_pass=top_settings["integrations.aarhus_los.ftp_pass"],
        ftp_folder=top_settings.get(
            "integrations.aarhus_los.ftp_folder", "TEST"
        ),
        import_state_file=top_settings["integrations.aarhus_los.state_file"],
    )
