import os
from enum import Enum, auto, unique
from pathlib import Path
from typing import Optional

# this env var is also used in bash
_env_path = os.environ.get('CUSTOMER_SETTINGS', None)
if _env_path:
    _env_path = Path(_env_path)


@unique
class PathDefaultMethod(Enum):
    raw = auto()
    cwd = auto()


def get_settings(method: Optional[PathDefaultMethod] = None) -> Path:
    """
    attempt at step towards unifying settings.json access
    :param method: How to find customer settings if missing
    :return: Path object to settings.json
    """
    if _env_path:
        return _env_path

    if method is None:
        raise Exception('Missing settings path. '
                        'Ensure environment variables are set up properly')

    if method is PathDefaultMethod.raw:
        return Path("settings") / "settings.json"
    elif method is PathDefaultMethod.cwd:
        return Path.cwd() / "settings" / "settings.json"
    else:
        raise ValueError('Unexpected method: {}'.format(method))
