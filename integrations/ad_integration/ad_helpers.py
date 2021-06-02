import os

from os2mo_helpers.mora_helpers import MoraHelper

from .ad_logger import start_logging

MORA_BASE = os.environ.get("MORA_BASE")


def find_mo_engagements_without_ad(helper):
    """ "
    Find a list of all current MO engagements that do not have a valid AD account
    registred as an IT-system.
    """
    org = helper.read_organisation()
    query = "o/{}/e?limit=1000000000"
    employees = helper._mo_lookup(org, query)

    active_users = set()
    missing_in_ad = set()
    i = 0
    for employee in employees["items"]:
        i += 1
        print("{}/{}".format(i, len(employees["items"])))
        uuid = employee["uuid"]
        engagements = helper.read_user_engagement(uuid)
        if engagements:
            active_users.add(uuid)

    for employee in active_users:
        ad_user = helper.get_e_username(employee, "Active Directory")
        # Notice: mora_helpers returns an empty string (not None) if no
        # account is found.
        if ad_user == "":
            missing_in_ad.add(employee)
    return missing_in_ad


if __name__ == "__main__":
    start_logging("ad_helpers.log")
    helper = MoraHelper(hostname=MORA_BASE, use_cache=False)

    missing_in_ad = find_mo_engagements_without_ad(helper)
    print(len(missing_in_ad))
