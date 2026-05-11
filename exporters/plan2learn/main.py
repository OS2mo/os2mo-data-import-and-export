from exporters.plan2learn.ship_files import ship_files
from fastapi import APIRouter
from fastapi import FastAPI
from fastramqpi.main import FastRAMQPI

from exporters.plan2learn.plan2learn import main
from exporters.plan2learn.plan2learn_settings import Settings

router = APIRouter()

@router.post("/")
async def root() -> str:
    return "Plan2learn"

@router.post("/trigger")
async def trigger(speedup: bool = True, dry_run: bool = False) -> None:
    settings = Settings()
    print("Starting plan2learn export" + " - DRY-RUN: Not shipping files." if dry_run else "" )
    await main(speedup, settings)
    if not dry_run:
        ship_files(settings)

def create_fastramqpi() -> FastRAMQPI:
    settings = Settings()
    fastramqpi = FastRAMQPI(
        application_name="plan2learn",
        settings=settings.fastramqpi,
        graphql_version=29,
    )
    fastramqpi.add_context(settings=settings)

    # Add our HTTP router(s)
    app = fastramqpi.get_app()
    app.include_router(router)

    return fastramqpi


def create_app() -> FastAPI:
    fastramqpi = create_fastramqpi()
    return fastramqpi.get_app()
