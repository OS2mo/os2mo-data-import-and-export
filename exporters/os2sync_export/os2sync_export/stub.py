import json
import logging

logger = logging.getLogger(__name__)


class Session:
    headers: dict[str, str] = {}
    # Mock /hierarchy return as an uuid in .text
    text = "4abf7909-a30e-475c-a4e4-5e03347632fa"
    # Always return status 404 to make os2sync_export try to create all objects
    status_code = 404

    # Setup logging to a file to track all os2sync requests.
    # This allows us to check for changes in the requests when changing the code.
    logname = "os2sync_requests.txt"
    logger = logging.getLogger(logname)

    log_config = logging.FileHandler(logname, mode="w", encoding="UTF8")
    formatter = logging.Formatter("%(message)s")
    log_config.setFormatter(formatter)

    logger.addHandler(log_config)

    def raise_for_status(self):
        pass

    def get(self, *args, **kwargs):
        self.logger.info("GET %r %r", args, kwargs)
        return self

    def delete(self, *args, **kwargs):
        self.logger.info("DELETE %r %r", args, kwargs)
        return self

    def post(self, *args, **kwargs):
        self.logger.info("POST %r %r", args, json.dumps(kwargs))
        return self

    def json(self):
        return {"Result": {"OUs": [], "Users": []}}
