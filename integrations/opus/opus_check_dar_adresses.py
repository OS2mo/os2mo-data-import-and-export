import logging

import click
from fastramqpi.ra_utils.tqdm_wrapper import tqdm

from integrations import dawa_helper
from integrations.opus.opus_helpers import get_latest_dump
from integrations.opus.opus_helpers import load_settings
from integrations.opus.opus_helpers import read_and_transform_data

logger = logging.getLogger("OpusInvalidDar")


@click.command()
def cli():
    settings = load_settings()
    _, opus_dump = get_latest_dump()
    org_units, _, _, _, _ = read_and_transform_data(
        None,
        opus_dump,
        filter_ids=settings["integrations.opus.units.filter_ids"],
    )
    invalid_addresses = []
    for unit in tqdm(org_units):
        if unit.get("street") and unit.get("zipCode"):
            address_uuid = dawa_helper.dawa_lookup(unit["street"], unit["zipCode"])
            if not address_uuid:
                invalid_addresses.append(
                    (unit["@id"], unit["longName"], unit["street"], unit["zipCode"])
                )
    click.echo(invalid_addresses)


if __name__ == "__main__":
    cli()
