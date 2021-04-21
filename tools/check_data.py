from exporters.utils.load_settings import load_settings
from tools.data_fixers.find_duplicate_users import check_duplicate_cpr
from tools.data_fixers.remove_duplicate_classes import find_duplicates_classes


def main():
    """Run checks on MO data"""

    settings = load_settings()
    mox_base = settings.get("mox.base", "http://localhost:8080/")
    session = requests.Session()

    dup = find_duplicates_classes(session=session, mox_base=mox_base)
    if dup:
        raise Exception("There are duplicate classes")

    common_cpr = check_duplicate_cpr()
    if common_cpr:
        raise Exception("There are multiple users with the same CPR-number")


if __name__ == "__main__":
    main()
