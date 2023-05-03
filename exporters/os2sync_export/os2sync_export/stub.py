import json
import logging
from uuid import uuid4


logger = logging.getLogger(__name__)


class Session:
    headers: dict[str, str] = {}
    text = str(uuid4())
    status_code = 404
    logname = "os2sync_requests.txt"
    logger = logging.getLogger(logname)

    log_config = logging.FileHandler(logname, mode="w")
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
