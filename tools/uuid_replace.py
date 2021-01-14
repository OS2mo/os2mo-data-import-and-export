import json
from itertools import filterfalse
from uuid import UUID

import click
from more_itertools import flatten

from exporters.utils.multiple_replace import multiple_replace


def is_valid_uuid(uuid_to_test, version=None):
    """Check if uuid_to_test is a valid UUID.

    Examples:
        >>> is_valid_uuid('c9bf9e57-1685-4c89-bafb-ff5af830be8a')
        True
        >>> is_valid_uuid('c9bf9e58')
        False
        >>> is_valid_uuid(True)
        False
        >>> is_valid_uuid(11)
        False

    Args:
        uuid_to_test: The string to test for validity.
        version: The UUID version to test against {None, 1, 2, 3, 4}.

    Returns:
        `True` if uuid_to_test is a valid UUID, `False` otherwise.
    """
    try:
        uuid_obj = UUID(str(uuid_to_test), version=version)
    except ValueError:
        return False
    return str(uuid_obj) == uuid_to_test


@click.command()
@click.argument("input", type=click.File("r"))
@click.argument("jsonmap", type=click.File("r"))
@click.argument("output", type=click.File("w"))
def transform(input, jsonmap, output):
    """Convert all UUIDs from JSONMAP in INPUT and write the result to OUTPUT.

    JSONMAP is a json file containing key-value pairs of UUIDs, the key being
    the original UUID and the value being the UUID to replace it with.
    """
    mapping = json.load(jsonmap)
    # Verify that keys and values are all valid UUIDs
    entries = flatten(mapping.items())
    entries = filterfalse(is_valid_uuid, entries)
    for entry in entries:
        raise click.ClickException("Found non-UUID value in jsonmap: " + entry)

    # Read the entire input, do multiple replace, and write entire output
    input_string = input.read()
    output_string = multiple_replace(input_string, mapping)
    output.write(output_string)


if __name__ == "__main__":
    transform()
