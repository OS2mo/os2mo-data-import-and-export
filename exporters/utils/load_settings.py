import json
import pathlib
from functools import lru_cache


@lru_cache(maxsize=None)
def load_settings():
    cfg_file = pathlib.Path.cwd() / "settings" / "settings.json"
    if not cfg_file.is_file():
        raise Exception("No settings file: " + str(cfg_file))
    return json.loads(cfg_file.read_text())
