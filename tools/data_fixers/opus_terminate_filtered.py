from operator import itemgetter

import click
import httpx
from integrations.opus import opus_helpers
from integrations.opus.opus_diff_import import OpusDiffImport
from ra_utils.load_settings import load_settings
from tqdm import tqdm

from tools.data_fixers.remove_from_lora import delete_object_and_orgfuncs


def terminate_filtered_units(dry_run):
    settings = load_settings()
    filter_ids = settings.get("integrations.opus.units.filter_ids", [])
    latest_date, opus_dump = opus_helpers.get_latest_dump()
    file_diffs = opus_helpers.file_diff(None, opus_dump)
    filtered_units, _ = opus_helpers.filter_units(file_diffs["units"], filter_ids)
    diff = OpusDiffImport(latest_date, ad_reader=None, employee_mapping={})
    mo_units = list(diff.find_unterminated_filtered_units(filtered_units))
    diff.handle_filtered_units(mo_units, dry_run=dry_run)
    return mo_units


def terminate_filtered_employees(dry_run):
    settings = load_settings()
    filter_ids = settings.get("integrations.opus.units.filter_ids", [])
    mox_base = settings.get("mox.base", "localhost:8080")
    latest_date, opus_dump = opus_helpers.get_latest_dump()
    file_diffs = opus_helpers.file_diff(None, opus_dump)
    # Get every id of filtered units
    all_ids = opus_helpers.find_all_filtered_ids(opus_dump, filter_ids)
    # find all engagements to a filtered unit in latest opus-file
    filtered_employees = list(
        filter(lambda emp: emp.get("orgUnit") in all_ids, file_diffs["employees"])
    )
    diff = OpusDiffImport(latest_date, ad_reader=None, employee_mapping={})
    # Check if any engagements exist that should have been filtered
    eng_info = [
        diff._find_engagement(e["@id"], "Engagement", present=False)
        for e in filtered_employees
    ]
    eng_info = list(filter(lambda x: x is not None, eng_info))
    if dry_run:
        print(
            f"There are {len(eng_info)} engagements that should have been terminated."
        )
        return eng_info

    for eng_uuid in tqdm(eng_info, desc="Deleting filtered engagements"):
        r = httpx.delete(f"{mox_base}/organisation/organisationfunktion/{eng_uuid}")
        r.raise_for_status()

    # Check users in MO - if no engagements are left then delete the user and all details to it.
    user_cprs = set(map(opus_helpers.read_cpr, filtered_employees))
    users = [diff.helper.read_user(user_cpr=cpr) for cpr in user_cprs]
    users = filter(lambda x: x, users)
    user_uuids = set(map(itemgetter("uuid"), users))

    eng = list(map(diff.helper.read_user_engagements, user_uuids))
    eng_uuid = dict(zip(user_uuids, eng))
    delete_users = dict(filter(lambda x: x[1] == [], eng_uuid.items()))
    for user_uuid in tqdm(
        delete_users, desc="Deleting users with no other engagements"
    ):
        delete_object_and_orgfuncs(
            uuid=user_uuid, mox_base=mox_base, object_type="bruger", dry_run=dry_run
        )


@click.command()
@click.option(
    "--dry-run",
    is_flag=True,
    help="Terminate Opus objects that have been filtered.",
)
def cli(dry_run):
    """Terminate Opus units that are filtered.

    This tool terminates any unit and employee from Opus that has been filtered. This can be usefull when a new unit is added to the filter.
    """
    terminate_filtered_units(dry_run)
    terminate_filtered_employees(dry_run)


if __name__ == "__main__":
    cli()
