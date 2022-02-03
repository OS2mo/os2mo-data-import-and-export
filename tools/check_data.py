import requests

from ra_utils.load_settings import load_settings
from tools.data_fixers.find_duplicate_users import check_duplicate_cpr
from tools.data_fixers.class_tools import find_duplicates_classes
from tools.data_fixers.opus_terminate_filtered_units import (
    terminate_filtered_units,
    terminate_filtered_employees
)


def main():
    """Run checks on MO data"""

    settings = load_settings()
    mox_base = settings.get("mox.base", "http://localhost:8080/")
    mora_base = settings.get("mora.base", "http://localhost:5000/")

    session = requests.Session()

    dup = find_duplicates_classes(session=session, mox_base=mox_base)
    assert not dup, f"There are {len(dup)} duplicate classes"

    common_cpr = check_duplicate_cpr(mora_base=mora_base)
    assert not common_cpr, f"There are {len(common_cpr)} users with the same CPR-number"

    if settings.get("crontab.RUN_OPUS_DIFF_IMPORT"):
        unfiltered_units = list(terminate_filtered_units(dry_run=True))
        assert not unfiltered_units, f"Found {len(unfiltered_units)} unit(s) that should have been filtered: {[u['name'] for u in unfiltered_units]}"

        unfiltered_employees = terminate_filtered_employees(dry_run=True)
        asser unfiltered_employees == 0, f"Found {unfiltered_employees} engagements that should have been filtered"



if __name__ == "__main__":
    main()
