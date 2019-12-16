import os

import flask
import logging
import subprocess
import toml
import uuid

app = flask.Flask(__name__)

logger = logging.getLogger(__name__)


@app.route("/")
def index():
    return flask.render_template("index.html")


@app.route("/submit", methods=["POST"])
def submit():
    args = flask.request.form.get("uuid")
    split_args = args.split(" ")

    if len(split_args) != 1:
        return "Parameter must be a single UUID"

    try:
        uuid_arg = uuid.UUID(split_args[0])
    except ValueError:
        return "UUID is malformed"

    script = app.config.get("script").split(" ")
    script.append(str(uuid_arg))

    try:
        result = subprocess.run(script)
        result.check_returncode()
    except (OSError, subprocess.CalledProcessError):
        logger.exception("Script error occurred")
        return "Script error occurred"

    return flask.render_template("index.html")


def create_app():
    with open("config.toml") as f:
        config = toml.loads(f.read())
        app.config.update(config)
        os.environ["SCRIPT_NAME"] = config.get("script_name")

    return app
