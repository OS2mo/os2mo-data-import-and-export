# SPDX-FileCopyrightText: Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
import datetime
import sys

from sdlon.fix_departments import unit_fixer

sys.path.insert(0, "/")
import json
from os.path import exists
from typing import Any, Dict, List
from uuid import UUID

from fastapi import BackgroundTasks, FastAPI
from fastapi.responses import RedirectResponse
from os2mo_fastapi_utils.tracing import setup_instrumentation, setup_logging
from os2mo_http_trigger_protocol import (
    EventType,
    MOTriggerPayload,
    MOTriggerRegister,
    RequestType,
)
from structlog import get_logger
from structlog.processors import KeyValueRenderer

from sdtool.config import get_settings

logger = get_logger()


tags_metadata: List[Dict[str, Any]] = [
    {
        "name": "Meta",
        "description": "Various meta endpoints",
    },
    {
        "name": "SDTool API",
        "description": "Old School SDTool API.",
    },
    {
        "name": "Trigger API",
        "description": "Trigger API for mo-triggers.",
        "externalDocs": {
            "description": "OS2MO Trigger docs",
            "url": "https://os2mo.readthedocs.io/en/development/api/triggers.html",
        },
    },
]
app = FastAPI(
    title="SDTool",
    description="API to update MO according to SD.",
    openapi_tags=tags_metadata,
)


@app.on_event("startup")
async def startup_event():
    # Called for validation side-effect
    get_settings()


@app.get(
    "/", response_class=RedirectResponse, tags=["Meta"], summary="Redirect to /docs"
)
def root() -> RedirectResponse:
    """Redirect to /docs."""
    return RedirectResponse(url="/docs")


@app.get("/info", tags=["Meta"], summary="Print info about this entity")
def info() -> Dict[str, Any]:
    """Print info about this entity."""
    return {
        "title": app.title,
        "description": app.description,
        "version": app.version,
    }


def ensure_settings_file():
    settings_path = "/opt/os2mo-data-import-and-export/settings/settings.json"
    if exists(settings_path):
        logger.debug("Early return from ensure_settings_file")
        return False

    settings = get_settings().dict()
    settings["sd_password"] = get_settings().sd_password.get_secret_value()

    settings_mapping = {
        "crontab.SAML_TOKEN": "saml_token",
        "mora.base": "mora_url",
        "integrations.SD_Lon.sd_user": "sd_user",
        "integrations.SD_Lon.sd_password": "sd_password",
        "integrations.SD_Lon.base_url": "sd_base_url",
        "integrations.SD_Lon.institution_identifier": "sd_institution_identifier",
        "integrations.SD_Lon.import.too_deep": "sd_too_deep",
    }
    dipex_settings = {
        dipex_key: str(settings[app_key])
        for dipex_key, app_key in settings_mapping.items()
        if settings[app_key] is not None
    }

    logger.info("Creating dipex settings file")
    with open(settings_path, "w") as settings_file:
        json.dump(dipex_settings, settings_file)
    return True


@app.get(
    "/triggers",
    tags=["Trigger API"],
    summary="List triggers to be registered.",
    response_model=List[MOTriggerRegister],
    response_description=(
        "Successful Response" + "<br/>" + "List of triggers to register."
    ),
)
def triggers():
    """List triggers to be registered."""
    return [
        {
            "event_type": EventType.ON_BEFORE,
            "request_type": RequestType.REFRESH,
            "role_type": "org_unit",
            "url": "/triggers/ou/refresh",
            "timeout": 60,
        }
    ]


@app.post(
    "/triggers/ou/refresh",
    tags=["Trigger API"],
    summary="Update the specified MO unit according to SD data",
    response_model=Dict[str, str],
    response_description=("Successful Response" + "<br/>" + "Script output."),
)
async def triggers_ou_refresh(payload: MOTriggerPayload, bg_tasks: BackgroundTasks):
    """Update the specified MO unit according to SD data"""
    logger.info("SDTool called", payload=payload)
    bg_tasks.add_task(unit_fixer, UUID(payload.request["uuid"]))
    logger.info("Background task started")

    start_time = datetime.datetime.now().strftime("%H:%M")
    return {
        "msg": f"SD-Tool opdatering påbegyndt {start_time}. Genindlæs siden om nogle minutter."
    }


app = setup_instrumentation(app)

from structlog.contextvars import merge_contextvars

setup_logging(processors=[merge_contextvars, KeyValueRenderer()])
