from tools.data_fixers.find_duplicate_users import check_duplicate_cpr
from tools.data_fixers.remove_duplicate_classes import check_duplicates_classes


def main():

    dup = check_duplicates_classes()
    if dup > 0:
        raise Exception("There are duplicate classes")

    common_cpr = check_duplicate_cpr()
    if common_cpr:
        raise Exception("There are multiple users with the same CPR-number")


if __name__ == "__main__":
    main()
