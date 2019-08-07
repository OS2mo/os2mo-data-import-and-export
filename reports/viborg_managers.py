#
# Copyright (c) 2017-2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

import os
import logging
from anytree import PostOrderIter, PreOrderIter
from os2mo_helpers.mora_helpers import MoraHelper
from pdb import set_trace as breakpoint

MORA_BASE = os.environ.get('MORA_BASE', 'http://localhost:80')
MORA_ROOT_ORG_UNIT_NAME = os.environ.get('MORA_ROOT_ORG_UNIT_NAME', 'Viborg Kommune')
LOG_LEVEL = logging._nameToLevel.get(os.environ.get('LOG_LEVEL', 'WARNING'), 20)
REPORT_OUTFILE = os.environ.get('REPORT_OUTFILE', 'viborg_managers.csv')


logging.basicConfig(
    format='%(levelname)s %(asctime)s %(name)s %(message)s',
    level=LOG_LEVEL,
)

logger = logging.getLogger("viborg_managers")
for i in logging.root.manager.loggerDict:
    if i in ["mora-helper", "viborg_managers"]:
        logging.getLogger(i).setLevel(LOG_LEVEL)
    else:
        logging.getLogger(i).setLevel(logging.WARNING)


def hourly_paid(engagement):
    "workers hourly paid are determined by user_key prefix"
    hp = engagement["user_key"][0] in ["8", "9"]
    if hp:
        logger.info("engagement %s with user_key %s considered hourly paid",
                    engagement["uuid"], engagement["user_key"])
    return hp

def find_people(mh, nodes):

    """ forberedelser. der laves et rapport-dictionary på hver node
    det er dictionaries, da det ikke er tal vi opbevarer,
    men medarbejder-uuid-nøgler
    """
    for node in PostOrderIter(nodes['root']):
        node.report = {
            "department": {},
            "m_dir_salary":{},
            "m_tot_salary":{},
            "e_dir_salary":{},
            "e_dir_hourly":{},
            "e_tot_salary":{},
            "e_tot_hourly":{},
        }

    for node in PostOrderIter(nodes['root']):
        report = node.report
        ou = mh.read_ou(node.name)
        report["department"] = ou["uuid"]

        """ find denne afdelings managere og registrer dem hver især
        med person-uuid - som nøgle
        gør også det nødvendige for at finde deres egen afdeling
        """
        mana = False
        for manager in mh._mo_lookup(
                ou["uuid"],
                'ou/{}/details/manager'
        ):
            if not manager.get("person"):
                logger.info("skipping vacant manager %s", mana)
                continue  # vacant manager
            else:
                mana = manager["person"]["uuid"]

            # se på et engagement for at finde egen afd
            for engagement in mh._mo_lookup(
                    mana,
                    'e/{}/details/engagement'
                )[:1]:
                # gem begge dele for senere brug
                payload={
                    "manager":manager,
                    "engagement":engagement
                }
                report["m_dir_salary"].setdefault(mana,payload)

        """ find alle engagementer i denne afdeling og opbevar
        medarbejdernes uuider som nøgler i dicts for henholdsvis
        timelønnede og funktionærer
        """

        for engagement in mh._mo_lookup(
                ou["uuid"],
                'ou/{}/details/engagement'
        ):
            empl_e = engagement["person"]["uuid"]
            empl_ou = engagement["org_unit"]
            if hourly_paid(engagement):
                report["e_dir_hourly"].setdefault(empl_e, empl_ou)
            else:
                report["e_dir_salary"].setdefault(empl_e, empl_ou)

        """ overfør de, som er ansat direkte i denne afdeling til
        afdelingens total- og tæl managers med i denne sammentælling
        """

        node.report["e_tot_salary"].update(report["m_dir_salary"])
        node.report["e_tot_salary"].update(report["e_dir_salary"])
        node.report["e_tot_hourly"].update(report["e_dir_hourly"])

        """ overfør nu alle medarbejderuuider, så de også indgår i parentens
            dict. PostOrderIter sikrer at alle tal propagerer hele vejen op
        """

        if node.parent:
            # all employees must go to total above
            node.parent.report["e_tot_salary"].update(report["e_tot_salary"])
            node.parent.report["e_tot_hourly"].update(report["e_tot_hourly"])

            # all managers must be direct employees above
            node.parent.report["m_tot_salary"].update(report["m_dir_salary"])
            node.parent.report["e_dir_salary"].update(report["m_dir_salary"])

            # no managers here ? all are are direct empls of next above manager
            if not mana:
                node.parent.report["m_dir_salary"].update(report["m_dir_salary"])
                node.parent.report["e_dir_salary"].update(report["e_dir_salary"])
                node.parent.report["e_dir_hourly"].update(report["e_dir_hourly"])

def output_report(mh, nodes, report_outfile):
    fieldnames = ["Leder", "Egen afd", "Email", "Direkte funktionær",
                  "Heraf ledere", "Direkte timeløn", "Direkte ialt",
                  "Samlet funktionær", "Samlet timeløn", "Samlet ialt"]
    rows = []
    for node in PreOrderIter(nodes['root']):

        """ 'manager' is the person-uuid of the person who is manager in this dept
        'payload' contains his manager and engagement-objects
        """

        for manager, payload  in node.report["m_dir_salary"].items():
            _email = mh.get_e_address(manager, "EMAIL")


            row={
                "Leder": payload["manager"]["person"]["name"],
                "Egen afd": payload["manager"]["org_unit"]["name"],
                "Email": ( _email.get("name", "") if _email.get(
                          "visibility", {}
                         ).get("scope", "") != "SECRET" else 'hemmelig'),
                "Direkte funktionær": len([
                    (k,v) for k, v in node.report["e_dir_salary"].items()
                    if k != payload["manager"]["person"]["uuid"]
                ]),
                "Heraf ledere": len([
                    (k,v) for k, v in node.report["m_tot_salary"].items()
                    if k != payload["manager"]["person"]["uuid"]
                ]),
                "Direkte timeløn": len(node.report["e_dir_hourly"]),
                "Samlet funktionær": len([
                    (k,v) for k, v in node.report["e_tot_salary"].items()
                    if k != payload["manager"]["person"]["uuid"]
                ]),
                "Samlet timeløn": len(node.report["e_tot_hourly"]),
            }
            row.update({
                "Direkte ialt": row["Direkte funktionær"] + row["Direkte timeløn"],
                "Samlet ialt": row["Samlet funktionær"] + row["Samlet timeløn"],
            })
            rows.append(row)
    mh._write_csv(fieldnames, rows, report_outfile)


def main(
    report_outfile,
    root_org_unit_name=MORA_ROOT_ORG_UNIT_NAME,
    mh=MoraHelper(),
):
    root_org_unit_uuid = None
    org = mh.read_organisation()
    roots = mh.read_top_units(org)
    for root in roots:
        logger.debug("checking if %s is %s", root["name"], root_org_unit_name)
        if root['name'] == root_org_unit_name:
            root_org_unit_uuid = root['uuid']
            break

    if not root_org_unit_uuid:
        logger.error("%s not found in root-ous", root_org_unit_name)
        exit(1)

    logger.warning("caching all ou's,"
                   " so program may seem unresponsive temporarily")
    nodes = mh.read_ou_tree(root_org_unit_uuid)
    find_people(mh, nodes)
    output_report(mh, nodes, report_outfile)



if __name__ == '__main__':
    morah = MoraHelper(MORA_BASE)
    main(report_outfile=REPORT_OUTFILE, mh=morah)
