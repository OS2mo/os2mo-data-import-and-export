import json
import logging
from uuid import uuid4


logger = logging.getLogger(__name__)


class Session:
    headers: dict[str, str] = {}
    text = str(uuid4())
    status_code = 404

    def raise_for_status(self):
        pass

    def get(self, *args, **kwargs):
        logger.info("GET %r %r", args, kwargs)
        return self

    def delete(self, *args, **kwargs):
        logger.info("DELETE %r %r", args, kwargs)
        return self

    def post(self, *args, **kwargs):
        logger.info("POST %r %r", args, json.dumps(kwargs))
        return self

    def json(self):
        return {"Result": {"OUs": [], "Users": []}}
