import click

from sd_importer import cli as sd_importer
from test_mo_against_sd import cli as mo_against_sd
from test_sd_connectivity import check_connectivity
from calculate_primary import calculate_primary
from db_overview import read_rundb
from fix_departments import unit_fixer
from sync_job_id import sync_jobid
from sd_fixup import cli as sd_fixup
from sd_changed_at import changed_at


@click.group()
def SDTool():
    """Common entrypoint to SD programs."""
    pass


SDTool.add_command(sd_importer, "sd_importer")
SDTool.add_command(mo_against_sd, "test_mo_against_sd")
SDTool.add_command(check_connectivity)
SDTool.add_command(calculate_primary)
SDTool.add_command(read_rundb)
SDTool.add_command(unit_fixer)
SDTool.add_command(sync_jobid)
SDTool.add_command(changed_at)
SDTool.add_command(sd_fixup)
SDTool.add_command(sd_importer, "sd_importer")


if __name__ == "__main__":
    SDTool()
