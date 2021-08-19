from os import listdir
from os.path import isfile, join
from functools import partial
from itertools import compress
from typing import Tuple
import json

import click


def list_files(path: str) -> Tuple[str]:
    """Return a tuple of filenames under path."""
    subpaths = listdir(path)
    # Add prefix to subpaths, and check if they are files or not
    prefixed_subpaths = map(partial(join, path), subpaths)
    is_file = map(isfile, prefixed_subpaths)
    # Compress to only keep subpaths that are files
    return tuple(compress(subpaths, is_file))


def consume_file(path: str) -> str:
    """Open the file at path and return its contents."""
    with open(path, "r") as f:
        return f.read()


@click.command()
@click.argument('paths', nargs=-1, required=True)
def generate(paths: Tuple[str]):
    """Script to generate settings.json from Kubernetes mounted ConfigMap."""
    dicty = {}

    for path in paths:
        files = list_files(path)
        for f in files:
            dicty[f] = consume_file(join(path, f))

    print(json.dumps(dicty, indent=4, sort_keys=True))


if __name__ == '__main__':
    generate()
