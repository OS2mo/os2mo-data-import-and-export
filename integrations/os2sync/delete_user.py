from uuid import UUID

import click

from integrations.os2sync.os2sync import delete_user


@click.command()
@click.option(
    "--uuid",
    type=click.UUID,
    required=True,
    help="UUID of the MO user to delete in FK org.",
)
def delete_fk_org_user(uuid: UUID) -> None:
    """Delete a MO user in FK org."""
    delete_user(str(uuid))


if __name__ == "__main__":
    delete_fk_org_user()
