import logging.config
from pathlib import Path

from ra_utils.load_settings import load_settings

from .read_ad_conf_settings import read_settings


class PasswordRemovalFormatter(logging.Formatter):
    def __init__(self, *args, **kwargs):
        settings = kwargs.pop("settings")
        super().__init__(*args, **kwargs)
        self._passwords = set(self._get_passwords_from_settings(settings))

    def format(self, record):
        original = logging.Formatter.format(self, record)
        return self._remove_password(original)

    def _get_passwords_from_settings(self, settings):
        for key in ("primary", "global"):
            password = settings.get(key, {}).get("password")
            if password:
                yield password

    def _remove_password(self, s):
        for password in self._passwords:
            s = s.replace(password, "*" * len(password))
        return s


def start_logging(log_file, **kwargs):
    settings = load_settings()

    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": "%(levelname)s %(asctime)s %(name)s: %(message)s",
                "()": PasswordRemovalFormatter,
                "settings": kwargs.get("settings") or read_settings(),
            },
            "export": {
                "format": "%(asctime)s: %(message)s",
                "()": PasswordRemovalFormatter,
                "settings": kwargs.get("settings") or read_settings(),
            },
        },
        "handlers": {
            # Local logging to file in the DIPEX folder, specified by `log_file`
            "local": {
                "formatter": "default",
                "class": "logging.FileHandler",
                "filename": log_file,
            },
            # Export logs to the MO queries folder
            "export": {
                "formatter": "export",
                "class": "logging.FileHandler",
                "filename": Path(settings["mora.folder.query_export"], log_file),
            },
        },
        "loggers": {
            "": {
                "handlers": ["local"],
                "level": "DEBUG",
            },
            "export": {
                "handlers": ["export"],
                "level": "ERROR",
            },
            "urllib3": {
                "level": "WARNING",
            },
        },
    }

    logging.config.dictConfig(config)
