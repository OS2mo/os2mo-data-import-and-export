import click

from calculate_primary import calculate_primary
from db_overview import read_rundb
from fix_departments import unit_fixer
from sd_changed_at import changed_at
from sd_fixup import cli as sd_fixup
from sd_importer import cli as sd_importer
from sd_mox import sd_mox_cli
from sd_tree_org import sd_tree_org
from sync_job_id import sync_jobid
from test_mo_against_sd import cli as mo_against_sd
from test_sd_connectivity import check_connectivity


@click.group()
def SDTool():
    """Common entrypoint to SD programs."""
    pass


SDTool.add_command(sd_importer, "sd-importer")
SDTool.add_command(mo_against_sd, "test-mo-against-sd")
SDTool.add_command(check_connectivity)
SDTool.add_command(calculate_primary)
SDTool.add_command(read_rundb)
SDTool.add_command(unit_fixer)
SDTool.add_command(sync_jobid)
SDTool.add_command(changed_at)
SDTool.add_command(sd_fixup, "sd-fixup")
SDTool.add_command(sd_importer, "sd-importer")
SDTool.add_command(sd_tree_org)
SDTool.add_command(sd_mox_cli)


if __name__ == "__main__":
    SDTool()
