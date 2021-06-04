# Copyright (c) 2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Viborg er den eneste, der anvender emus-eksporten endnu,
# så når vi laver en cacheversion kan vi pensionere den gamle langsomme
#

import logging
import sys
import io
import collections
import datetime
import time
import requests
from xml.sax.saxutils import escape
from functools import partial
from itertools import filterfalse

from exporters.emus import config

import click
from tqdm import tqdm
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_

from anytree import Node, PreOrderIter

from exporters.sql_export.lc_for_jobs_db import get_engine  # noqa
from exporters.sql_export.sql_table_defs import (Adresse, Bruger, ItSystem,
                                                 Engagement, Enhed, Leder,
                                                 LederAnsvar, ItForbindelse)
from exporters.utils.priority_by_class import lcdb_choose_public_address


logging.basicConfig(
    format=config.logformat,
    filename=config.logfile,
    level=config.loglevel,
)

logger = logging.getLogger(config.loggername)

for i in logging.root.manager.loggerDict:
    if i in [config.loggername]:
        logging.getLogger(i).setLevel(config.loglevel)
    else:
        logging.getLogger(i).setLevel(logging.WARNING)

engagement_counter = collections.Counter()
dar_cache = {}


def get_session(engine):
    return sessionmaker(bind=engine, autoflush=False)()


def engagement_count(nodes, ou):
    ouid = ou.uuid
    while ouid:
        engagement_counter[ouid] += 1
        if nodes[ouid].parent:
            ouid = nodes[ouid].parent.unit.uuid
        else:
            ouid = None


def get_dar_address(db_address):
    if not db_address:
        address = {}
    else:
        dar_uuid = db_address.dar_uuid
        if dar_cache.get(dar_uuid) is None:
            dar_cache[dar_uuid] = {}
            for addrtype in ('adresser', 'adgangsadresser'):
                logger.debug('Looking up dar: {}'.format(dar_uuid))
                adr_url = 'https://dawa.aws.dk/{}'.format(addrtype)
                # 'historik/adresser', 'historik/adgangsadresser'
                params = {'id': dar_uuid, 'struktur': 'mini'}
                # Note: Dar accepts up to 10 simultanious
                # connections, consider grequests.
                counter = 0
                max_tries = 10
                while True:
                    counter += 1
                    try:
                        r = requests.get(url=adr_url, params=params)
                        address_data = r.json()
                        break
                    except json.decoder.JSONDecodeError:
                        print(r.text)
                        continue
                    if counter > max_tries:
                       raise Exception("DAR does not respond!")
                    time.sleep(5)

                if address_data:
                    dar_cache[dar_uuid] = address_data[0]
                    break
                dar_cache[dar_uuid] = {}
        else:
            logger.debug('Cache hit dar: {}'.format(dar_uuid))
        address = dar_cache[dar_uuid]
    return {
        'zipCode': address.get("postnr", ""),
        'city': address.get("postnrnavn", ""),
        'street': address.get("betegnelse", "")
    }


def read_ou_tree(session, org, nodes={}, parent=None):
    """Recursively find all sub-ou's beneath current node
    :param org: The top org unit to start the tree from
    :param nodes: Dict with all modes in the tree
    :param parent: The parent of the current node, None if this is root
    :return: A dict with all nodes in tree, top node is named 'root'
    """

    if parent is None:
        org_unit = session.query(Enhed).filter(Enhed.uuid == org).one()
        parent = nodes[org] = nodes['root'] = Node(org, unit=org_unit)


    units = session.query(Enhed).filter(Enhed.forældreenhed_uuid == org)
    for unit in units:
        uuid = unit.uuid
        nodes[uuid] = Node(uuid, parent=parent, unit=unit)
        nodes = read_ou_tree(session, uuid, nodes, nodes[uuid])
    return nodes


def export_ou_emus(session, nodes, emus_file=sys.stdout):
    """
        we need uuid, validity, name, parent_uuid, manager, street, zip, city, phone
        for
    """
    fieldnames = ['startDate', 'endDate', 'parentOrgUnit', 'manager',
                  'longName', 'street', 'zipCode', 'city', 'phoneNumber']

    rows = []
    for node in tqdm(PreOrderIter(nodes['root']), total=len(nodes), desc="export ou"):
        ou = node.unit
        if not engagement_counter[ou.uuid]:
            logger.info("skipping dept %s with no non-hourly-paid employees",
                        ou.uuid)
            continue

        manager = session.query(Leder).filter(
            Leder.uuid == ou.leder_uuid
        ).first()

        manager_uuid = manager.bruger_uuid if manager else ''
        # Ensure that managers actually have an engagement
        if manager_uuid:
            entrydate, _ = get_manager_dates(session, manager_uuid)
            if entrydate is None:
                logger.info("skipping manager %s with no current employment (for ou)", manager_uuid)
                manager_uuid = ''

        # manager_uuid = ou.leder_uuid or ''

        street_address = session.query(Adresse).filter(and_(
            Adresse.adressetype_titel == 'Postadresse',
            Adresse.enhed_uuid == ou.uuid,
        )).one_or_none()
        address = get_dar_address(street_address)
        fra = ou.startdato or ''
        til = ou.slutdato or ''
        over_uuid = ou.forældreenhed_uuid or ''
        phone = session.query(Adresse.værdi).filter(and_(
            Adresse.adressetype_titel == 'Telefon',
            Adresse.enhed_uuid == ou.uuid,
        )).scalar()

        row = {
            'uuid': ou.uuid,
            'startDate': fra,
            'endDate': til,
            'parentOrgUnit': over_uuid,
            'manager': manager_uuid,
            'longName': ou.navn,
            'street': address.get("street", ''),
            'zipCode': address.get("zipCode", ''),
            'city': address.get("city", ''),
            'phoneNumber': phone or '',
        }
        logger.info("adding ou %s: %r", ou.uuid, row)
        rows.append(row)

    last_changed = datetime.datetime.now().strftime("%Y-%m-%d")
    logger.info("writing %d ou rows to file", len(engagement_counter))
    for r in rows:
        empls = engagement_counter[r["uuid"]]
        if empls == 0:
            logger.debug("empty department skipped: %s (%s)",
                         r["longName"], r["uuid"])
        else:
            logger.debug("department %s (%s) has %s engagements incl. subdepts.",
                         r["longName"], r["uuid"], empls)
            emus_file.write(
                "<orgUnit id=\"%s\" client=\"1\" lastChanged=\"%s\">\n" % (
                    r["uuid"],
                    last_changed,
                )
            )

            for fn in fieldnames:
                emus_file.write("<%s>%s</%s>\n" % (fn, escape(r.get(fn, '')), fn))

            emus_file.write("</orgUnit>\n")


def get_e_address(e_uuid, scope, session, settings):
    candidates = session.query(Adresse).filter(and_(
        Adresse.adressetype_scope == scope,
        Adresse.bruger_uuid == e_uuid,
    )).all()

    if scope == "Telefon":
        priority_list = settings["EMUS_PHONE_PRIORITY"]
    elif scope == "E-mail":
        priority_list = settings["EMUS_EMAIL_PRIORITY"]
    else:
        priority_list = []

    address = lcdb_choose_public_address(candidates, priority_list)
    if address is not None:
        return address
    else:
        return {} # like mora_helpers


def build_engagement_row(session, settings, ou, engagement):
    entrydate = engagement.startdato
    leavedate = engagement.slutdato

    employee = session.query(Bruger).filter(
        Bruger.uuid == engagement.bruger_uuid
    ).one()

    firstname = employee.fornavn
    lastname = employee.efternavn

    username = session.query(ItForbindelse.brugernavn).filter(and_(
        ItForbindelse.bruger_uuid == engagement.bruger_uuid,
        ItForbindelse.it_system_uuid == settings["username-itsystem-uuid"]
    )).limit(1).scalar()

    _phone_obj = get_e_address(engagement.bruger_uuid, 'Telefon', session, settings)
    _phone = None
    if _phone_obj:
        _phone = _phone_obj.værdi

    _email_obj = get_e_address(engagement.bruger_uuid, 'E-mail', session, settings)
    _email = None
    if _email_obj:
        _email = _email_obj.værdi

    row = {
        'personUUID': engagement.bruger_uuid,
        # employee_id is tjenestenr by default
        'employee_id': engagement.bvn,
        # client is 1 by default
        'client': "1",
        'entryDate': entrydate if entrydate else '',
        'leaveDate': leavedate if leavedate else '',
        'cpr': employee.cpr,
        'firstName': firstname,
        'lastName': lastname,
        'workPhone': _phone or '',
        'workContract': engagement.engagementstype_uuid,
        'workContractText': engagement.engagementstype_titel,
        'positionId': engagement.stillingsbetegnelse_uuid,
        'position': engagement.stillingsbetegnelse_titel,
        'orgUnit': ou.uuid,
        'email': _email or '',
        'username': username or '',
    }
    return row


def get_manager_dates(session, bruger_uuid):
    """Man kan tydeligvis ikke regne med at chefens datoer
    på lederobjektet er korrekte. Derfor ser vi lige på
    om chefen fortsat er ansat, inden vi rapporterer.
    """
    # TODO: XXX: Hvorfor kan man ikke det, og burde vi ikke fikse det?
    startdate = '9999-12-31'
    enddate = '0000-00-00'
    for engagement in session.query(Engagement).filter(
        Engagement.bruger_uuid == bruger_uuid
    ).all():
        if engagement.slutdato and enddate != '':
            # Enddate is finite, check if it is later than current
            if engagement.slutdato > enddate:
                enddate = engagement.slutdato
        else:
            enddate = ''

        if engagement.startdato < startdate:
            startdate = engagement.startdato

    # difference from before, since lcdb has no past
    # assert startdate < '9999-12-31'
    # assert enddate == '' or enddate > '0000-00-00'
    if startdate == '9999-12-31':
        return None, None
    return startdate, enddate


def build_manager_rows(session, settings, ou, manager):
    # render manager returns a list as a manager will typically have more
    # responsibility areas and musskema requires one for each

    rows = []

    bruger = session.query(Bruger).filter(
        Bruger.uuid == manager.bruger_uuid
    ).one()

    firstname = bruger.fornavn
    lastname = bruger.efternavn

    entrydate, leavedate = get_manager_dates(session, bruger.uuid)
    if entrydate is None:
        logger.info("skipping manager %s with no current employment (for user)", manager.uuid)
        return []

    username = session.query(ItForbindelse.brugernavn).filter(and_(
        ItForbindelse.bruger_uuid == bruger.uuid,
        ItForbindelse.it_system_uuid == settings["username-itsystem-uuid"]
    )).limit(1).scalar()

    _phone_obj = get_e_address(bruger.uuid, 'Telefon', session, settings)
    _phone = None
    if _phone_obj:
        _phone = _phone_obj.værdi

    _email_obj = get_e_address(bruger.uuid, 'E-mail', session, settings)
    _email = None
    if _email_obj:
        _email = _email_obj.værdi

    # manipulate each responsibility row into a manager row
    # empty a couple of fields, change client and employee_id
    # and manipulate from and to

    for responsibility in session.query(LederAnsvar).filter(
        LederAnsvar.leder_uuid == manager.uuid,
    ).all():
        if not responsibility.lederansvar_uuid == settings[
                "EMUS_RESPONSIBILITY_CLASS"
        ]:
            logger.debug("skipping man. resp. %s", responsibility.lederansvar_titel)
            continue
        logger.info(
            "adding manager %s with man. resp. %s",
            manager.uuid,
            responsibility.lederansvar_titel
        )
        row = {
            'personUUID': bruger.uuid,
            'employee_id': bruger.uuid,
            'client': "540",
            'entryDate': entrydate,
            'leaveDate': leavedate,
            'cpr': bruger.cpr,
            'firstName': firstname,
            'lastName': lastname,
            'workPhone': _phone or '',
            'workContract': '',
            'workContractText': '',
            'positionId': '',
            'position': responsibility.lederansvar_titel,
            'orgUnit': '',
            'email': _email or '',
            'username': username or '',
        }
        rows.append(row)
    return rows


def hourly_paid(engagement):
    "workers hourly paid are determined by user_key prefix"
    hp = engagement.bvn[0] in ["8", "9"]
    if hp:
        logger.info("engagement %s with user_key %s considered hourly paid",
                    engagement.uuid, engagement.bvn)
    return hp


def discarded(settings, engagement):
    jfkey = engagement.stillingsbetegnelse_uuid
    etuuid = engagement.engagementstype_uuid
    if etuuid not in settings["EMUS_ALLOWED_ENGAGEMENT_TYPES"]:
        logger.debug("%s discarded engagement_type %s",
                     engagement.bruger_uuid, etuuid)
        return True
    if jfkey in settings["EMUS_DISCARDED_JOB_FUNCTIONS"]:
        logger.debug("%s discarded job function %s", engagement.bruger_uuid, jfkey)
        return True
    return False


def export_e_emus(session, settings, nodes, emus_file):
    fieldnames = ['personUUID', 'entryDate', 'leaveDate', 'cpr', 'firstName',
                  'lastName', 'workPhone', 'workContract', 'workContractText',
                  'positionId', 'position', "orgUnit", 'email', "username"]
    manager_rows = []
    engagement_rows = []

    settings["username-itsystem-uuid"] = session.query(ItSystem.uuid).filter(
        ItSystem.navn == "Active Directory"
    ).scalar()

    for node in tqdm(PreOrderIter(nodes['root']), total=len(nodes), desc="export e"):
        ou = node.unit

        # normal engagements - original export
        engagements = session.query(Engagement).filter(
            Engagement.enhed_uuid == ou.uuid
        ).all()
        engagements = filterfalse(hourly_paid, engagements)
        engagements = filterfalse(partial(discarded, settings), engagements)
        for engagement in engagements:
            logger.info("adding engagement %s", engagement.uuid)
            engagement_rows.append(build_engagement_row(session, settings,
                                                        ou, engagement))
            engagement_count(nodes, ou)

        # manager engagements - above mentioned musskema adaptation
        for manager in session.query(Leder).filter(
            Leder.enhed_uuid == ou.uuid
        ).all():
            if manager.bruger_uuid is None:
                logger.info("skipping vacant manager %s", manager.uuid)
                continue  # vacant manager
            else:
                # extend, as there can be zero or one
                manager_rows.extend(build_manager_rows(session, settings,
                                                       ou, manager))

    if not len(manager_rows):
        logger.error("no managers found - did You forget to"
                     " specify correct EMUS_RESPONSIBILITY_CLASS")
    rows = engagement_rows + manager_rows
    logger.info("writing %d engagement rows and %d manager rows to file",
                len(engagement_rows), len(manager_rows))

    last_changed = datetime.datetime.now().strftime("%Y-%m-%d")
    for r in rows:
        emus_file.write("<employee id=\"%s\" client=\"%s\" lastChanged=\"%s\">\n" % (
            r["employee_id"],
            r["client"],
            last_changed,
        ))
        for fn in fieldnames:
            emus_file.write("<%s>%s</%s>\n" % (fn, escape(r.get(fn, '')), fn))
        emus_file.write("</employee>\n")


def main(emus_xml_file, settings):
    session = get_session(get_engine())
    nodes = read_ou_tree(session, settings["MORA_ROOT_ORG_UNIT_UUID"])

    # Write employees to temp file, counting to determine if ou included
    temp_file = io.StringIO()
    export_e_emus(session, settings, nodes, temp_file)

    # Begin the xml file
    emus_xml_file.write("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n")
    emus_xml_file.write("<OS2MO>\n")

    # write included units to xml-file
    export_ou_emus(session, nodes, temp_file)

    # Write employees to xml file
    emus_xml_file.write(temp_file.getvalue())

    # End xml file
    emus_xml_file.write("</OS2MO>")


@click.command()
@click.argument('filename')
def cli(filename):
    with open(filename, "w", encoding="utf-8") as emus_f:
        main(emus_xml_file=emus_f, settings=config.settings)


if __name__ == '__main__':
    cli()
