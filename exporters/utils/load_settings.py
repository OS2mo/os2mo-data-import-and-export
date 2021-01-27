import json
from functools import lru_cache
from pathlib import Path
from functools import lru_cache


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
    cwd = Path().cwd().absolute()
    settings_path = cwd / "settings" / "settings.json"
    with open(str(settings_path), "r") as settings_file:
        return json.load(settings_file)


if __name__ == "__main__":
    print(json.dumps(load_settings(), indent=4))
