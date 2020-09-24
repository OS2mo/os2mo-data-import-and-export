import pathlib
import json

from .utils import LazyDict


def _load_settings_from_disk():
    # TODO: Handle run schema against settings.json
    # TODO: Handle datatype for specific keys?
    cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
    if not cfg_file.is_file():
        print("No settings file")
        exit(1)

    try:
        settings = json.loads(cfg_file.read_text())
    except json.decoder.JSONDecodeError as e:
        print('Syntax error in settings file: {}'.format(e))
        exit(1)
    return settings


_SETTINGS = LazyDict()
_SETTINGS.set_initializer(_load_settings_from_disk)


def get_settings():
    return _SETTINGS
