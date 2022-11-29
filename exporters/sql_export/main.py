# SPDX-FileCopyrightText: Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""Integration entrypoint."""
# TODO: This is mostly copied from FastRAMQPI, when we can use Python 3.10 in DIPEX
#       consider switching over to that instead.
from typing import Any

from fastapi import FastAPI
from prometheus_client import Info
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseSettings
from pydantic import Field

from .sql_export import SqlExportSettings
from .trigger import trigger_router

build_information = Info("build_information", "Build information")


class Settings(BaseSettings):
    """Settings for the SQLExport FastAPI application."""

    class Config:
        """Settings are frozen."""

        frozen = True

    # We assume these will be set by the docker build process,
    # and as such will contain release information at runtime.
    commit_tag: str = Field("HEAD", description="Git commit tag.")
    commit_sha: str = Field("HEAD", description="Git commit SHA.")

    enable_metrics: bool = Field(True, description="Whether to enable metrics.")


def update_build_information(version: str, build_hash: str) -> None:
    """Update build information.

    Args:
        version: The version to set.
        build_hash: The build hash to set.

    Returns:
        None.
    """
    build_information.info(
        {
            "version": version,
            "hash": build_hash,
        }
    )


def create_app(**kwargs: Any) -> FastAPI:
    """FastAPI application factory.

    Args:
        kwargs: Various settings overrides.

    Returns:
        FastAPI application.
    """
    pydantic_settings = SqlExportSettings()
    pydantic_settings.start_logging_based_on_settings()

    settings = Settings(**kwargs)

    app = FastAPI(
        title="sql_export",
        version=settings.commit_tag,
        contact={
            "name": "Magenta Aps",
            "url": "https://www.magenta.dk/",
            "email": "info@magenta.dk>",
        },
        license_info={
            "name": "MPL-2.0",
            "url": "https://www.mozilla.org/en-US/MPL/2.0/",
        },
    )

    @app.get("/")
    async def index() -> dict[str, str]:
        return {"name": "sql_export"}

    app.include_router(trigger_router)

    if settings.enable_metrics:
        # Update metrics info
        update_build_information(
            version=settings.commit_tag, build_hash=settings.commit_sha
        )

        Instrumentator().instrument(app).expose(app)

    return app
