import click
from integrations.opus import opus_helpers
from integrations.opus.opus_diff_import import OpusDiffImport
from ra_utils.load_settings import load_settings


def terminate_filtered_units(terminate):
    settings = load_settings()
    filter_ids = settings.get("integrations.opus.units.filter_ids", [])
    dumps = opus_helpers.read_available_dumps()
    latest_date = max(dumps.keys())
    file_diffs = opus_helpers.file_diff(None, dumps[latest_date])
    filtered_units, _ = opus_helpers.filter_units(file_diffs["units"], filter_ids)
    diff = OpusDiffImport(latest_date, ad_reader=None, employee_mapping={})
    diff.handle_filtered_units(filtered_units, terminate=terminate)


@click.command()
@click.option(
    "--delete",
    type=click.BOOL,
    default=False,
    is_flag=True,
    required=False,
    help="Terminate Opus units that have been filtered.",
)
def cli(delete):
    """Terminate Opus units that are filtered.

    This tool terminates any unit from Opus that has been filtered. This can be usefull when a new unit is added to the filter
    or if a unit has been moved below a filterd unit in Opus before this functionallity existed
    """
    terminate_filtered_units(delete)


if __name__ == "__main__":
    cli()
