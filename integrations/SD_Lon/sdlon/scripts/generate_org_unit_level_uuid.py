from uuid import UUID

import click

from sdlon.sd_common import generate_uuid

def _get_class_uuid(department_level_identifier: str) -> UUID:
    """
    Generate org_unit_level class UUID according to the way it is
    done by the SD-importer. The UUID is necessary when manually creating
    new org_unit_levels after the initial SD-import is done.

    Args:
        department_level_identifier: the SD DepartmentLevelIdentifier for
          the org_unit_level, e.g. "NY7-niveau"

    Returns:
        UUID that the org_unit_level class has to be created with
    """
    class_uuid: str = generate_uuid(
        department_level_identifier,
        None,
        "Silkeborg Kommune"
    )
    return UUID(class_uuid)


@click.command()
# The SD DepartmentLevelIdentifier for the department
@click.argument("department_level_identifier")
def get_class_uuid(department_level_identifier: str) -> None:
    click.echo(_get_class_uuid(department_level_identifier))


if __name__ == "__main__":
    get_class_uuid()
