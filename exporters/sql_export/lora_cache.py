import logging
import pickle
import typing
from typing import Tuple

import click
from dateutil import tz
from ra_utils.job_settings import JobSettings

from .gql_lora_cache_async import GQLLoraCache
from .old_lora_cache import OldLoraCache

logger = logging.getLogger(__name__)

DEFAULT_TIMEZONE = tz.gettz("Europe/Copenhagen")

PICKLE_PROTOCOL = pickle.DEFAULT_PROTOCOL


class LoraCacheSettings(JobSettings):
    use_new_cache: bool = False

    class Config:
        settings_json_prefix = ""


def get_cache(resolve_dar=True, full_history=False, skip_past=False, settings=None):
    settings = settings or LoraCacheSettings()

    if isinstance(settings, LoraCacheSettings) and settings.use_new_cache:
        # If using the new cache, use the new type of settings, which it reads itself
        return GQLLoraCache(
            resolve_dar=resolve_dar, full_history=full_history, skip_past=skip_past
        )
    else:
        return OldLoraCache(
            resolve_dar=resolve_dar,
            full_history=full_history,
            skip_past=skip_past,
            settings=settings,
        )


def fetch_loracache() -> Tuple[
    typing.Union[OldLoraCache, GQLLoraCache], typing.Union[OldLoraCache, GQLLoraCache]
]:
    # Here we should activate read-only mode, actual state and
    # full history dumps needs to be in sync.

    # Full history does not calculate derived data, we must
    # fetch both kinds.
    lc = get_cache(resolve_dar=True, full_history=False)
    lc.populate_cache(skip_associations=True)
    lc.calculate_derived_unit_data()
    lc.calculate_primary_engagements()

    # Todo, in principle it should be possible to run with skip_past True
    # This is now fixed in a different branch, remember to update when
    # merged.
    lc_historic = get_cache(resolve_dar=True, full_history=True, skip_past=False)
    lc_historic.populate_cache(skip_associations=True)
    # Here we should de-activate read-only mode
    return lc, lc_historic


@click.command()
@click.option("--historic/--no-historic", default=True, help="Do full historic export")
@click.option(
    "--skip-past", is_flag=True, default=False, help="Skip past in historic export"
)
@click.option(
    "--resolve-dar/--no-resolve-dar",
    default=False,
    envvar="RESOLVE_DAR",
    help="Resolve DAR addresses",
)
@click.option("--read-from-cache", is_flag=True)
def cli(historic, skip_past, resolve_dar, read_from_cache):
    LoraCacheSettings().start_logging_based_on_settings()
    lc = get_cache(
        full_history=historic,
        skip_past=skip_past,
        resolve_dar=resolve_dar,
    )
    lc.populate_cache(dry_run=read_from_cache)

    logger.info("Now calcualate derived data")
    lc.calculate_derived_unit_data()
    lc.calculate_primary_engagements()


if __name__ == "__main__":

    cli()
