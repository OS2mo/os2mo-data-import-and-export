from fastramqpi.ra_utils.load_settings import load_settings

from tools.data_fixers.class_tools import find_duplicate_classes
from tools.data_fixers.find_duplicate_users import check_duplicate_cpr
from tools.data_fixers.opus_terminate_filtered import terminate_filtered_employees
from tools.data_fixers.opus_terminate_filtered import terminate_filtered_units


def main():
    """Run checks on MO data"""

    settings = load_settings()
    mora_base = settings.get("mora.base", "http://localhost:5000/")

    dup = find_duplicate_classes(
        mora_base=mora_base,
        client_id=settings["crontab.CLIENT_ID"],
        client_secret=settings["crontab.CLIENT_SECRET"],
        auth_realm="mo",
        auth_server=settings["crontab.AUTH_SERVER"],
    )
    assert not dup, f"There are {len(dup)} duplicate classes: {dup}"

    common_cpr = check_duplicate_cpr(mora_base=mora_base)
    assert not common_cpr, f"There are {len(common_cpr)} users with the same CPR-number"

    if settings.get("crontab.RUN_OPUS_DIFF_IMPORT"):
        unfiltered_units = list(terminate_filtered_units(dry_run=True))
        assert not unfiltered_units, (
            f"Found {len(unfiltered_units)} unit(s) that should have been filtered."
        )

        unfiltered_employees = terminate_filtered_employees(dry_run=True)
        assert not unfiltered_employees, (
            f"Found {len(unfiltered_employees)} engagements that should have been filtered"
        )


if __name__ == "__main__":
    main()
