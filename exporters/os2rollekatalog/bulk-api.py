from functools import lru_cache
from operator import itemgetter

import constants
import deepdiff
import requests
from os2mo_tools import mo_api
from ra_utils.catchtime import catchtime
from ra_utils.headers import TokenSettings
from ra_utils.load_settings import load_settings

from os2rollekatalog_integration import get_org_units


@lru_cache
def bulk_lookup(mora_base, endpoint):
    r = requests.get(f"{mora_base}/api/v1/{endpoint}")
    r.raise_for_status()
    return r.json()


def old_org(mora_base, main_root_org_unit):

    service_url = mora_base + "/service"
    mo_connector = mo_api.Connector(service_url, org_uuid=True)
    mo_connector.org_id = mo_connector._get_org()
    org_units = get_org_units(mo_connector, main_root_org_unit, "cpr_mo_ad_map.csv")
    return org_units


def new_org(mora_base, main_root_org_unit):
    org_units = bulk_lookup(mora_base, "org_unit")
    org_unit_uuids = map(itemgetter("uuid"), org_units)
    unit_map = dict(zip(org_unit_uuids, org_units))
    managers = bulk_lookup(mora_base, "manager")
    it_systems = bulk_lookup(mora_base, "it")
    person_map = {
        it["person"]["uuid"]: it["user_key"]
        for it in it_systems
        if it["itsystem"]["name"] == constants.AD_it_system
    }

    payload = []
    for manager in managers:
        org_unit_uuid = manager["org_unit"]["uuid"]
        person_uuid = manager["person"]["uuid"]
        samaccountname = person_map.get(person_uuid)
        manager = None
        if samaccountname:
            manager = {"uuid": person_uuid, "userId": samaccountname}
    
        payload.append(
            {
                "uuid": manager["org_unit"]["uuid"],
                "name": manager["org_unit"]["name"],
                "parentOrgUnitUuid": unit_map[org_unit_uuid]["uuid"],
                "manager": manager,
            }
        )
    return payload


def test_same(mora_base, main_root_org_unit):
    with catchtime(True) as t:
        old = old_org(mora_base, main_root_org_unit)
    time_spent, process_time = t()
    print(time_spent)  
    print(process_time)  

    with catchtime(True) as t:
        new = new_org(mora_base, main_root_org_unit)
    time_spent, process_time = t()
    print(time_spent)  
    print(process_time)

    diff = deepdiff.DeepDiff(old, new)
    print(diff)


if __name__ == "__main__":

    settings = load_settings()
    mora_base = settings.get("mora.base")
    main_root_org_unit = settings.get("exporters.os2rollekatalog.main_root_org_unit")

    test_same(mora_base, main_root_org_unit)
