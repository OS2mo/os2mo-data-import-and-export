import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Optional

# this env var is also used in bash
_env_path = os.environ.get("CUSTOMER_SETTINGS", None)

if _env_path:
    _env_path = Path(_env_path)


def get_settings_path(method: Optional[int] = None) -> Path:
    """
    attempt at step towards unifying settings.json access
    :param method: How to find customer settings if missing
    :return: Path object to settings.json
    """
    if _env_path:
        return _env_path

    if method is None:
        raise Exception(
            "Missing settings path. Ensure environment variables are set up properly"
        )

    if method == 0:
        return Path("settings") / "settings.json"
    elif method == 1:
        return Path.cwd() / "settings" / "settings.json"
    elif method == 2:
        return Path().cwd().absolute() / "settings" / "settings.json"
    else:
        raise ValueError("Unexpected method: {}".format(method))


@lru_cache(maxsize=None)
def load_settings():
    """Load settings file from settings/settings.json.

    This function is in-memory cached using lru_cache, such that the underlying file
    is only read and parsed once, thus if the settings file is written to / updated
    after a program has called this function once, it will not return the new values.

    If this is needed, the cache must first be invalidated using a call to clear_cache:

        load_setings.clear_cache()

    Returns:
        json: The parsed settings file.
    """
    settings_path = get_settings_path(method=3)
    with open(str(settings_path), "r") as settings_file:
        return json.load(settings_file)


if __name__ == "__main__":
    print(json.dumps(load_settings(), indent=4))
