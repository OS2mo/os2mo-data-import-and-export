import json
from functools import lru_cache

from pydantic import BaseModel

from customer_settings import get_settings
from customer_settings import PathDefaultMethod

class Settings(BaseModel):
    ftp_url: str
    ftp_user: str
    ftp_pass: str
    import_state_file: str
    mox_base: str
    mora_base: str
    saml_token: str
    queries_dir: str


@lru_cache()
def get_config():
    settingsfile = get_settings(PathDefaultMethod.raw)
    top_settings = json.loads(settingsfile.read_text())

    return Settings(
        ftp_url=top_settings.get(
            "integrations.aarhus_los.ftp_url", "ftp.aarhuskommune.dk"
        ),
        ftp_user=top_settings["integrations.aarhus_los.ftp_user"],
        ftp_pass=top_settings["integrations.aarhus_los.ftp_pass"],
        import_state_file=top_settings["integrations.aarhus_los.state_file"],
        mox_base=top_settings["mox.base"],
        mora_base=top_settings["mora.base"],
        saml_token=top_settings["crontab.SAML_TOKEN"],
        queries_dir=top_settings["mora.folder.query_export"],
    )
