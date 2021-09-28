import click
from os2mo_helpers.mora_helpers import MoraHelper
from ra_utils.load_settings import load_setting


@click.command()
@click.option(
    "--mora_base", default=load_setting("mora.base", "http://localhost:5000/")
)
@click.option("--bvn", required=True)
@click.option("--facet", required=True)
@click.option("--title")
@click.option("--uuid", type=click.UUID)
@click.option("--scope", default="TEXT")
def ensure_class_in_facet(mora_base, bvn, facet, title, uuid, scope):
    """Creates a class if it doesn't allready exist

    Example:
        metacli ensure_class_exists --bvn=Orlov --facet=leave_type

    Returns the uuid of the created/existing class.
    """
    helper = MoraHelper(hostname=mora_base, use_cache=False)
    title = title or bvn
    assert all(arg != "" for arg in (bvn, title, facet, scope)), "Inputs can't be empty"
    class_uuid = helper.ensure_class_in_facet(
        facet=facet, bvn=bvn, title=title, uuid=uuid, scope=scope
    )
    if uuid:
        assert (
            class_uuid == uuid
        ), f"This class allready existed with another uuid {class_uuid}"
    click.echo(class_uuid)


if __name__ == "__main__":
    ensure_class_in_facet()
