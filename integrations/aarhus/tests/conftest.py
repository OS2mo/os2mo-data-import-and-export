import logging

import pytest


logger = logging.getLogger()


@pytest.fixture(autouse=True)
def ensure_logging_framework_not_altered():
    # Fix/work around I/O error when running tests:
    #
    # --- Logging error ---
    # Traceback (most recent call last):
    #   File "/usr/lib/python3.10/logging/__init__.py", line 1103, in emit
    #     stream.write(msg + self.terminator)
    # ValueError: I/O operation on closed file.
    # Call stack:
    #   File "/home/mrw/src/os2mo-data-import-and-export/.venv/lib/python3.10/site-packages/aiohttp/client.py", line 348, in __del__
    #     self._loop.call_exception_handler(context)
    #   File "/usr/lib/python3.10/asyncio/base_events.py", line 1778, in call_exception_handler
    #     self.default_exception_handler(context)
    #   File "/usr/lib/python3.10/asyncio/base_events.py", line 1752, in default_exception_handler
    #     logger.error('\n'.join(log_lines), exc_info=exc_info)
    # Message: 'Unclosed client session\nclient_session: <aiohttp.client.ClientSession object at 0x7f539a729c30>'
    # Arguments: ()
    #
    # Source: https://github.com/pytest-dev/pytest/issues/14#issuecomment-521577819
    before_handlers = list(logger.handlers)
    yield
    logger.handlers = before_handlers
