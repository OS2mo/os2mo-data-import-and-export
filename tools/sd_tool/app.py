import os

import flask
import logging
import subprocess
import toml
from uuid import UUID

app = flask.Flask(__name__)

logger = logging.getLogger(__name__)


@app.route("/", methods=["POST"])
def index():
    uuid = flask.request.json.get("uuid")

    if not uuid:
        return flask.jsonify("Parameter must be a single UUID"), 400

    try:
        UUID(uuid)
    except ValueError:
        return flask.jsonify("UUID is malformed"), 400

    script = app.config.get("script").split(" ")
    script.append(str(uuid))

    try:
        result = subprocess.run(
            script, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding="UTF-8"
        )
        result.check_returncode()
    except OSError as e:
        logger.exception("Script error occurred")
        return flask.jsonify(e.strerror), 500
    except subprocess.CalledProcessError as e:
        return flask.jsonify(e.stdout), 500

    return flask.jsonify({"output": result.stdout})


def create_app():
    with open("config.toml") as f:
        config = toml.loads(f.read())
        app.config.update(config)
        os.environ["SCRIPT_NAME"] = config.get("script_name")

    return app
