"""
This script offers access to the DIPEX command line tools that use Click.

Installation
============

This script can be installed into a virtualenv like this::

    $ . venv/bin/activate
    $ pip install --editable .

Then it should be available as an ordinary shell command::

    $ metacli --help

Autocompletion
==============

Click autocompletion can be activated like this::

    $ eval "$(_METACLI_COMPLETE=source_bash metacli)"

(For other shells, see `the Click documentation
<https://click.palletsprojects.com/en/7.x/bashcomplete/#activation>`_)

Then, you should be able to autocomplete subcommand names like this::

    $ metacli sd_<TAB><TAB>
      sd_changed_at        sd_fixup.fixup_all       sd_fixup.fixup_department
      sd_fixup.fixup_user  sd_importer.full_import  sd_importer.import_user

Option names should also be autocompleted.
"""

import importlib
import inspect
import logging
import os
import sys
from operator import itemgetter
from typing import Callable, Dict, List, Optional, Tuple

import click

from exporters.utils.apply import apply


ROOT_FOLDER = os.path.abspath(os.path.dirname(__file__))

logger = logging.getLogger(__name__)

class MetaCLI(click.MultiCommand):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Use counter to construct subcommand names, in case multiple Python
        # modules use the same filename (e.g. "calculate_primary".)
        # TODO: Remove this counter and associated code once
        # https://git.magenta.dk/rammearkitektur/os2mo-data-import-and-export/-/merge_requests/383
        # is merged.
        self._counter = 1

        # Some DIPEX tools need PYTHONPATH to be set up in a particular way
        # before they can be imported. Do it once here to avoid adding many
        # identical paths to PYTHONPATH.
        # TODO: Remove this.
        self._add_to_sys_path()

    def list_commands(self, ctx: click.Context) -> List[click.Command]:
        commands = self._build_command_map(ctx)
        return sorted(commands)

    def get_command(self, ctx: click.Context, name: str) -> click.Command:
        commands = self._build_command_map(ctx)
        if name in commands:
            return commands[name]
        raise click.ClickException('unknown subcommand %r' % name)

    def _build_command_map(self, ctx: click.Context):
        if hasattr(ctx, '_command_map'):
            return ctx._command_map
        else:
            ctx._command_map = {}

        def gen_root_and_file(it):
            for root, files in it:
                for name in files:
                    yield root, name

        @apply
        def skip_virtualenv(root, filename):
            return 'venv' not in root

        @apply
        def skip_ourselves(root, filename):
            return 'metacli' not in filename

        @apply
        def skip_non_python(root, filename):
            return filename.endswith('.py')

        # Generator of tuples of root and list of filenames
        root_and_files = map(itemgetter(0, 2), os.walk(ROOT_FOLDER))

        # Generator of tuples of root and filename
        root_and_file = gen_root_and_file(root_and_files)

        # Filter away virtualenv, this file itself, and non-Python files
        root_and_file = filter(skip_virtualenv, root_and_file)
        root_and_file = filter(skip_ourselves, root_and_file)
        root_and_file = filter(skip_non_python, root_and_file)

        # At this point, we only have Python files outside of the virtualenv
        modules = map(apply(self._get_module_path), root_and_file)
        modules = filter(None.__ne__, modules)

        # Add one or more Click commands from each matching Python module to
        # the command map.
        for modname, modpath in modules:
            self._add_module_commands(ctx._command_map, modname, modpath)

        return ctx._command_map

    def _get_module_path(self, root: str, name: str) -> Optional[Tuple[str, str]]:
        path = os.path.join(root, name)
        with open(path) as contents:
            # Skip any file not matching 'click.command' and 'cli.command'.
            if '.command' not in contents.read():
                return
            # Otherwise, turn filesystem path into a Python module path.
            # E.g. '/path/project/foo/bar.py' is turned into 'foo.bar'.
            pypath = root.replace(ROOT_FOLDER + '/', '')
            pypath = pypath.replace('/', '.')
            modname = name.replace('.py', '')
            modpath = f'{pypath}.{modname}'
            return modname, modpath

    def _add_module_commands(
        self,
        command_map: Dict[str, click.Command],
        modname: str,
        modpath: str,
    ):
        cmds = self._get_module_commands(modpath)
        if len(cmds) == 1:
            # There is exactly one command in the module.
            # Use the module name as subcommand name.
            cmdname = modname
            if modname in command_map:
                # The command name is already in use.
                # Construct a new name by appending `.1`, etc.
                cmdname = '%s.%d' % (modname, self._counter)
                self._counter += 1
            cmd = cmds[0][1]
            cmd.name = cmdname  # set name for autocomplete
            command_map[cmdname] = cmd
        else:
            # There are multiple commands in the module.
            # Command 'fixup_all' in 'sd_fixup' module is made available as
            # subcommand 'sd_fixup.fixup_all', etc.
            for funcname, cmd in cmds:
                cmdname = '%s.%s' % (modname, funcname)
                cmd.name = cmdname  # set name for autocomplete
                command_map[cmdname] = cmd

    def _get_module_commands(self, modpath: str) -> List[Tuple[str, click.Command]]:
        @apply
        def is_click_command(name: str, member: Callable) -> bool:
            return (
                isinstance(member, click.Command)
                and not isinstance(member, click.Group)
            )

        try:
            module = importlib.import_module(modpath)
        except Exception as e:
            logger.error('failed to import %s (exception: %r)', modpath, e)
            return []
        else:
            return list(filter(is_click_command, inspect.getmembers(module)))

    def _add_to_sys_path(self):
        # TODO: This should not be necessary, but currently DIPEX code in some
        # folders assume that it will run with *that folder* as the implicit
        # PYTHONPATH.
        additional_paths = [
            # for local imports in 'ad_integration',
            './integrations/ad_integration',
            # for code importing 'common_queries'
            './exporters',
        ]
        for path in additional_paths:
            sys.path.append(path)


cli = MetaCLI(help=__doc__)


if __name__ == '__main__':
    cli()
