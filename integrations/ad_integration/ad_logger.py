import logging.config

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
    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": "%(levelname)s %(asctime)s %(name)s: %(message)s",
                "()": PasswordRemovalFormatter,
                "settings": kwargs.get("settings") or read_settings(),
            },
        },
        "handlers": {
            "file": {
                "formatter": "default",
                "class": "logging.FileHandler",
                "filename": log_file,
            },
        },
        "loggers": {
            "": {
                "handlers": ["file"],
                "level": "DEBUG",
            },
            "urllib3": {
                "level": "WARNING",
            },
        },
    }
    logging.config.dictConfig(config)
