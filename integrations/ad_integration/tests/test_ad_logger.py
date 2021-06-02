import logging

from ..ad_logger import PasswordRemovalFormatter


def test_password_removal(caplog):
    logger = logging.getLogger("testing")
    password = "hunter2"
    settings = {"global": {"password": password}}
    formatter = PasswordRemovalFormatter(settings=settings)

    # Capture logs on the "testing" logger at level DEBUG or higher
    caplog.set_level(logging.DEBUG, "testing")
    # Patch pytest logging to use our custom log formatter
    caplog.handler.setFormatter(formatter)

    logger.debug("password: %r", password)
    assert password not in caplog.text
