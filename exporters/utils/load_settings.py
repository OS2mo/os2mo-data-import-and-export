import json
<<<<<<< HEAD
from functools import lru_cache
from pathlib import Path
=======
import pathlib
from functools import lru_cache
>>>>>>> bc1c9eb (Initial work on unifying primary)


@lru_cache(maxsize=None)
def load_settings():
<<<<<<< HEAD
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
=======
    cfg_file = pathlib.Path.cwd() / "settings" / "settings.json"
    if not cfg_file.is_file():
        raise Exception("No settings file: " + str(cfg_file))
    return json.loads(cfg_file.read_text())
>>>>>>> bc1c9eb (Initial work on unifying primary)
