import pathlib
import json

from .utils import LazyDict


def _load_settings_from_disk():
    # TODO: Soon we have done this 4 times. Should we make a small settings
    # importer, that will also handle datatype for specific keys?
    cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
    if not cfg_file.is_file():
        raise Exception('No setting file')
    # TODO: This must be clean up, settings should be loaded by __init__
    # and no references should be needed in global scope.
    settings = json.loads(cfg_file.read_text())
    return settings


SETTINGS = LazyDict()
SETTINGS.set_initializer(_load_settings_from_disk)
