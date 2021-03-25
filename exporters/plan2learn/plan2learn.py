# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

"""
Helper class to make a number of pre-defined queries into MO
"""
import json
import time
import logging
import pathlib
import datetime
import argparse
from anytree import PreOrderIter
from operator import itemgetter
from functools import partial
from more_itertools import flatten, only

from os2mo_helpers.mora_helpers import MoraHelper
from exporters.sql_export.lora_cache import LoraCache
from exporters.utils.load_settings import load_settings
from exporters.utils.priority_by_class import choose_public_address, lc_choose_public_address

LOG_LEVEL = logging.DEBUG
LOG_FILE = 'plan2learn.log'

logger = logging.getLogger('plan2learn')

for name in logging.root.manager.loggerDict:
    if name in ('LoraCache',  'mora-helper', 'plan2learn'):
        logging.getLogger(name).setLevel(LOG_LEVEL)
    else:
        logging.getLogger(name).setLevel(logging.ERROR)

logging.basicConfig(
    format='%(levelname)s %(asctime)s %(name)s %(message)s',
    level=LOG_LEVEL,
    filename=LOG_FILE
)


SETTINGS = load_settings()

ACTIVE_JOB_FUNCTIONS = []  # Liste over aktive engagementer som skal eksporteres.


def get_e_address(e_uuid, scope, lc_historic, settings):
    # Iterator of all addresses in LoRa
    lora_addresses = lc_historic.addresses.values()
    lora_addresses = flatten(lora_addresses)
    # Iterator of all addresses for the current user
    lora_addresses = filter(
        lambda address: address['user'] == e_uuid,
        lora_addresses
    )
    # Iterator of all addresses for current user and correct scope
    lora_addresses = filter(
        lambda address: address['scope'] == scope,
        lora_addresses
    )

    if scope == "Telefon":
        priority_list = settings.get("plan2learn.email.phone", [])
    elif scope == "E-mail":
        priority_list = settings.get("plan2learn.email.priority", [])
    else:
        priority_list = []

    address = lc_choose_public_address(candidates, priority_list)
    if address is not None:
        return address
    else:
        return {} # like mora_helpers


def get_e_address_mo(e_uuid, scope, mh, settings):
    candidates = mh.get_e_addresses(e_uuid, scope)

    if scope == "PHONE":
        priority_list = settings.get("emus.phone.priority", [])
    elif scope == "EMAIL":
        priority_list = settings.get("emus.email.priority", [])
    else:
        priority_list = []

    address = choose_public_address(candidates, priority_list)
    if address is not None:
        return address
    else:
        return {} # like mora_helpers


def construct_bruger_row(user_uuid, cpr, name, email, phone):
    row = {
        'BrugerId': user_uuid,
        'CPR': cpr,
        'Navn': name,
        'E-mail': email or ""
        'Mobil': phone or ""
        'Stilling': None  # To be populated later
    }
    return row


allowed_engagement_types = SETTINGS[
    'exporters.plan2learn.allowed_engagement_types'
]


def export_bruger_lc(node, used_cprs, lc, lc_historic):
    # TODO: If this is to run faster, we need to pre-sort into units,
    # to avoid iterating all engagements for each unit.
    lora_engagements = lc_historic.engagements.values()
    lora_engagements = flatten(lora_engagements)
    lora_engagements = filter(lambda engv: engv['unit'] == node.name, lora_engagements)
    lora_engagements = filter(
        lambda engv: engv['engagement_type'] in allowed_engagement_types,
        lora_engagements
    )
    lora_user_uuids = map(itemgetter('user'), lora_engagements)
    rows = []
    for user_uuid in lora_user_uuids:
        user = lc.users[user_uuid][0]
        cpr = user['cpr']
        if cpr in used_cprs:
            # print('Skipping user: {} '.format(uuid))
            continue
        used_cprs.add(cpr)
        name = user['navn']

        _phone_obj = get_e_address(user_uuid, 'Telefon', lc_historic, SETTINGS)
        _phone = None
        if _phone_obj:
            _phone = _phone_obj.værdi

        _email_obj = get_e_address(user_uuid, 'E-mail', lc_historic, SETTINGS)
        _email = None
        if _email_obj:
            _email = _email_obj.værdi

        rows.append(construct_bruger_row(
            user_uuid, cpr, name, _email, _phone
        ))
    return rows, used_cprs


def export_bruger_mo(node, used_cprs, mh):
    employees = mh.read_organisation_people(
        node.name, split_name=False, read_all=True, skip_past=True
    )
    rows = []
    for uuid, employee in employees.items():
        if employee['engagement_type_uuid'] not in allowed_engagement_types:
            continue
        user_uuid = employee['Person UUID']
        name = employee['Navn']
        cpr = address['CPR-Nummer']
        if cpr in used_cprs:
            # print('Skipping user: {} '.format(uuid))
            continue
        used_cprs.add(cpr)

        _phone_obj = get_e_address_mo(user_uuid, 'PHONE', mh, SETTINGS)
        _phone = None
        if _phone_obj:
            _phone = _phone_obj.værdi

        _email_obj = get_e_address_mo(user_uuid, 'EMAIL', mh, SETTINGS)
        _email = None
        if _email_obj:
            _email = _email_obj.værdi

        rows.append(construct_bruger_row(
            user_uuid, cpr, name, _email, _phone
        ))
    return rows, used_cprs


def export_bruger(mh, nodes, lc, lc_historic):
    #  fieldnames = ['BrugerId', 'CPR', 'Navn', 'E-mail', 'Mobil', 'Stilling']
    if lc and lc_historic:
        bruger_exporter = partial(export_bruger_lc, lc=lc, lc_historic=lc_historic)
    else:
        bruger_exporter = partial(export_bruger_mo, mh=mh)

    used_cprs = set()
    rows = []
    for node in PreOrderIter(nodes['root']):
        new_rows, used_cprs = bruger_exporter(node, used_cprs)
        rows.extend(new_rows)

    # Turns out, we need to update this once we reach engagements
    # mh._write_csv(fieldnames, rows, filename)
    return rows


def _split_dar(address):
    gade = post = by = ''
    if address:
        gade = address.split(',')[0]
        post = address.split(',')[1][1:5]
        by = address.split(',')[1][6:]
    return gade, post, by


def export_organisation(mh, nodes, filename, lc=None):
    fieldnames = ['AfdelingsID', 'Afdelingsnavn', 'Parentid', 'Gade', 'Postnr', 'By']

    rows = []
    # Vi laver en liste over eksporterede afdelinger, så de som ikke er eksporterede
    # men alligevel har en leder, ignoreres i lederutrækket (typisk NY1 afdelinger).
    eksporterede_afdelinger = []

    for node in PreOrderIter(nodes['root']):
        if lc:
            for unit in lc.units.values():
                # Units are never terminated, we can safely take first value
                unitv = unit[0]
                if unitv['uuid'] != node.name:
                    continue

                level_uuid = unitv['level']
                level_titel = lc.classes[level_uuid]
                too_deep = SETTINGS['integrations.SD_Lon.import.too_deep']
                if level_titel['title'] in too_deep:
                    continue

                over_uuid = unitv['parent'] if unitv['parent'] else ''

                address = None
                for raw_address in lc.addresses.values():
                    if raw_address[0]['unit'] == unitv['uuid']:
                        if raw_address[0]['scope'] == 'DAR':
                            address = raw_address[0]['value']

                gade, post, by = _split_dar(address)
                eksporterede_afdelinger.append(unit[0]['uuid'])
                row = {
                    'AfdelingsID': unit[0]['uuid'],
                    'Afdelingsnavn': unit[0]['name'],
                    'Parentid': over_uuid,
                    'Gade': gade,
                    'Postnr': post,
                    'By': by
                }
                rows.append(row)

        else:
            ou = mh.read_ou(node.name)
            level = ou['org_unit_level']
            if level['name'] in SETTINGS['integrations.SD_Lon.import.too_deep']:
                continue

            over_uuid = ou['parent']['uuid'] if ou['parent'] else ''

            dar_address = mh.read_ou_address(node.name)
            gade, post, by = _split_dar(dar_address.get('Adresse'))

            eksporterede_afdelinger.append(ou['uuid'])
            row = {
                'AfdelingsID': ou['uuid'],
                'Afdelingsnavn': ou['name'],
                'Parentid': over_uuid,
                'Gade': gade,
                'Postnr': post,
                'By': by
            }
            rows.append(row)

    mh._write_csv(fieldnames, rows, filename)
    return eksporterede_afdelinger


def export_engagement(mh, filename, eksporterede_afdelinger, brugere_rows,
                      lc, lc_historic):
    fieldnames = ['EngagementId', 'BrugerId', 'AfdelingsId', 'AktivStatus',
                  'StillingskodeId', 'Primær', 'Engagementstype',
                  'StartdatoEngagement']

    allowed_engagement_types = SETTINGS[
        'exporters.plan2learn.allowed_engagement_types'
    ]

    rows = []

    # Keep a list of exported engagements to avoid exporting the same engagment
    # multiple times if it has multiple rows in MO.
    exported_engagements = []

    err_msg = 'Skipping {}, due to non-allowed engagement type'
    if lc and lc_historic:
        for employee_effects in lc.users.values():
            for eng in lc_historic.engagements.values():
                # We can consistenly access index 0, the historic export
                # is for the purpose of catching future engagements, not
                # to catch all validities
                engv = eng[0]

                # As this is not the historic cache, there should only be one user
                employee = employee_effects[0]

                if engv['user'] != employee['uuid']:
                    continue

                if engv['unit'] not in eksporterede_afdelinger:
                    msg = 'Unit {} is not included in the export'
                    logger.info(msg.format(engv['unit']))
                    continue

                if engv['engagement_type'] not in allowed_engagement_types:
                    logger.debug(err_msg.format(eng))
                    continue

                if engv['uuid'] in exported_engagements:
                    continue
                exported_engagements.append(engv['uuid'])

                valid_from = datetime.datetime.strptime(
                    engv['from_date'], '%Y-%m-%d'
                )
                active = valid_from < datetime.datetime.now()
                if active:
                    aktiv_status = 1
                    start_dato = ''
                else:
                    # Currently we always set engagment to active, even if it is not.
                    aktiv_status = 1
                    start_dato = engv['from_date']

                if engv['uuid'] in lc.engagements:
                    primary = lc.engagements[engv['uuid']][0]['primary_boolean']
                else:
                    # This is a future engagement, we accept that the LoRa cache will
                    # not provide the answer and search in MO.
                    mo_engagements = mh.read_user_engagement(
                        employee['uuid'], read_all=True,
                        skip_past=True, calculate_primary=True
                    )
                    primary = None
                    for mo_eng in mo_engagements:
                        if mo_eng['uuid'] == engv['uuid']:
                            primary = mo_eng['is_primary']
                    if primary is None:
                        msg = 'Warning: Unable to find primary for {}!'
                        logger.warning(msg.format(engv['uuid']))
                        print(msg.format(engv['uuid']))
                        primary = False
                if primary:
                    primær = 1
                    for bruger in brugere_rows:
                        if bruger['BrugerId'] == employee['uuid']:
                            udvidelse_2 = engv['extensions'].get('udvidelse_2')
                            if udvidelse_2:
                                bruger['Stilling'] = udvidelse_2
                            else:
                                job_function = engv['job_function']
                                stilling = lc.classes[job_function]['title']
                                bruger['Stilling'] = stilling
                else:
                    primær = 0

                stilingskode_id = engv['job_function']
                ACTIVE_JOB_FUNCTIONS.append(stilingskode_id)
                eng_type = lc.classes[engv['engagement_type']]['title']

                row = {
                    'EngagementId': engv['uuid'],
                    'BrugerId':  employee['uuid'],
                    'AfdelingsId': engv['unit'],
                    'AktivStatus': aktiv_status,
                    'StillingskodeId': stilingskode_id,
                    'Primær': primær,
                    'Engagementstype': eng_type,
                    'StartdatoEngagement': start_dato
                }
                rows.append(row)
    else:
        employees = mh.read_all_users()
        for employee in employees:
            logger.info('Read engagements for {}'.format(employee))
            engagements = mh.read_user_engagement(employee['uuid'], read_all=True,
                                                  skip_past=True,
                                                  calculate_primary=True)
            present_engagements = mh.read_user_engagement(employee['uuid'],
                                                          read_all=False,
                                                          calculate_primary=True)
            for eng in engagements:
                if eng['org_unit']['uuid'] not in eksporterede_afdelinger:
                    # Denne afdeling er ikke med i afdelingseksport.
                    continue

                if eng['engagement_type']['uuid'] not in allowed_engagement_types:
                    logger.debug(err_msg.format(eng))
                    continue

                if eng['uuid'] in exported_engagements:
                    continue
                exported_engagements.append(eng['uuid'])
                logger.info('New line in file: {}'.format(eng))

                valid_from = datetime.datetime.strptime(
                    eng['validity']['from'], '%Y-%m-%d'
                )

                active = valid_from < datetime.datetime.now()
                logger.info('Active status: {}'.format(active))
                if active:
                    aktiv_status = 1
                    start_dato = ''
                else:
                    # Currently we always set engagement to active, even if it
                    # is not.
                    aktiv_status = 1
                    start_dato = eng['validity']['from']

                if eng['is_primary']:
                    primær = 1

                    # If we have a present engagement, make sure this is the
                    # one we use.
                    if present_engagements:
                        for present_eng in present_engagements:
                            if not present_eng['uuid'] == eng['uuid']:
                                # This is a future engagement
                                continue
                            for bruger in brugere_rows:
                                if bruger['BrugerId'] == employee['uuid']:
                                    if eng['extension_2']:
                                        bruger['Stilling'] = eng['extension_2']
                                    else:
                                        bruger['Stilling'] = eng[
                                            'job_function']['name']
                    else:
                        for bruger in brugere_rows:
                            if bruger['BrugerId'] == employee['uuid']:
                                if eng['extension_2']:
                                    bruger['Stilling'] = eng['extension_2']
                                else:
                                    bruger['Stilling'] = eng['job_function']['name']
                else:
                    primær = 0

                stilingskode_id = eng['job_function']['uuid']
                ACTIVE_JOB_FUNCTIONS.append(stilingskode_id)

                row = {
                    'EngagementId': eng['uuid'],
                    'BrugerId':  employee['uuid'],
                    'AfdelingsId': eng['org_unit']['uuid'],
                    'AktivStatus': aktiv_status,
                    'StillingskodeId': stilingskode_id,
                    'Primær': primær,
                    'Engagementstype': eng['engagement_type']['name'],
                    'StartdatoEngagement': start_dato
                }

                rows.append(row)
    mh._write_csv(fieldnames, rows, filename)
    return brugere_rows


def export_stillingskode(mh, nodes, filename, lc=None):
    fieldnames = ['StillingskodeID', 'AktivStatus', 'Stillingskode',
                  'Stillingskode#']
    rows = []
    if lc:
        job_function_facet = None
        for uuid, facet in lc.facets.items():
            if facet['user_key'] == 'engagement_job_function':
                job_function_facet = uuid
        assert(uuid is not None)

        for klasse in lc.classes:
            if klasse['facet'] is not job_function_facet:
                continue

            if klasse['uuid'] not in ACTIVE_JOB_FUNCTIONS:
                continue

            row = {
                'StillingskodeID': klasse['uuid'],
                'AktivStatus': 1,
                'Stillingskode': klasse['title'],
                'Stillingskode#': klasse['uuid']
            }
            rows.append(row)
    else:
        stillinger = mh.read_classes_in_facet('engagement_job_function')

        for stilling in stillinger[0]:
            if stilling['uuid'] not in ACTIVE_JOB_FUNCTIONS:
                continue

            row = {
                'StillingskodeID': stilling['uuid'],
                'AktivStatus': 1,
                'Stillingskode': stilling['name'],
                'Stillingskode#': stilling['uuid']
            }
            rows.append(row)
    mh._write_csv(fieldnames, rows, filename)


def export_leder(mh, nodes, filename, eksporterede_afdelinger, lc=None):
    fieldnames = ['BrugerId', 'AfdelingsID', 'AktivStatus', 'Titel']
    rows = []
    for node in PreOrderIter(nodes['root']):
        if node.name not in eksporterede_afdelinger:
            # Denne afdeling er ikke med i afdelingseksport.
            continue

        if lc:
            for manager in lc.managers:
                if manager['unit'] != node.name:
                    continue

                row = {
                    'BrugerId': manager['user'],
                    'AfdelingsID': node.name,
                    'AktivStatus': 1,
                    'Titel': manager['Ansvar']
                }
                rows.append(row)
        else:
            manager = mh.read_ou_manager(node.name, inherit=False)
            if 'uuid' in manager:
                row = {
                    'BrugerId': manager.get('uuid'),
                    'AfdelingsID': node.name,
                    'AktivStatus': 1,
                    'Titel': manager['Ansvar']
                }
                rows.append(row)
    mh._write_csv(fieldnames, rows, filename)


def main(speedup, dry_run=False):
    t = time.time()

    mh = MoraHelper(hostname=SETTINGS['mora.base'], export_ansi=False)

    dest_folder = pathlib.Path(SETTINGS['mora.folder.query_export'])
    root_unit = SETTINGS['exporters.plan2learn.root_unit']

    if speedup:
        # Here we should activate read-only mode, actual state and
        # full history dumps needs to be in sync.

        # Full history does not calculate derived data, we must
        # fetch both kinds.
        lc = LoraCache(resolve_dar=True, full_history=False)
        lc.populate_cache(dry_run=dry_run, skip_associations=True)
        lc.calculate_derived_unit_data()
        lc.calculate_primary_engagements()

        lc_historic = LoraCache(resolve_dar=False, full_history=True,
                                skip_past=True)
        lc_historic.populate_cache(dry_run=dry_run, skip_associations=True)
        # Here we should de-activate read-only mode
    else:
        lc = None
        lc_historic = None

    # Todo: We need the nodes structure to keep a consistent output,
    # consider if the 70 seconds is worth the implementation time of
    # reading this from cache.
    nodes = mh.read_ou_tree(root_unit)

    brugere_rows = export_bruger(mh, nodes, lc, lc_historic)
    print('Bruger: {}s'.format(time.time() - t))
    logger.info('Bruger: {}s'.format(time.time() - t))

    filename = str(dest_folder / 'plan2learn_organisation.csv')
    eksporterede_afdelinger = export_organisation(mh, nodes, filename, lc)
    print('Organisation: {}s'.format(time.time() - t))
    logger.info('Organisation: {}s'.format(time.time() - t))

    filename = str(dest_folder / 'plan2learn_engagement.csv')
    brugere_rows = export_engagement(mh, filename, eksporterede_afdelinger,
                                     brugere_rows, lc, lc_historic)
    print('Engagement: {}s'.format(time.time() - t))
    logger.info('Engagement: {}s'.format(time.time() - t))

    filename = str(dest_folder / 'plan2learn_stillingskode.csv')
    export_stillingskode(mh, nodes, filename)
    print('Stillingskode: {}s'.format(time.time() - t))
    logger.info('Stillingskode: {}s'.format(time.time() - t))

    filename = str(dest_folder / 'plan2learn_leder.csv')
    export_leder(mh, nodes, filename, eksporterede_afdelinger)
    print('Leder: {}s'.format(time.time() - t))
    logger.info('Leder: {}s'.format(time.time() - t))

    # Now exported the now fully populated brugere.csv
    filename = str(dest_folder / 'plan2learn_bruger.csv')
    brugere_fieldnames = ['BrugerId', 'CPR', 'Navn', 'E-mail', 'Mobil', 'Stilling']
    mh._write_csv(brugere_fieldnames, brugere_rows, filename)

    print('Export completed')
    logger.info('Export completed')


def cli():
    parser = argparse.ArgumentParser(description='Choose backend')
    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument('--lora',  action='store_true')
    group.add_argument('--mo',  action='store_true')
    parser.add_argument('--read-from-cache',  action='store_true')

    args = vars(parser.parse_args())

    logger.info('Starting with args: {}'.format(args))

    if args['lora']:
        main(speedup=True, dry_run=args['read_from_cache'])

    elif args['mo']:
        main(speedup=False)
    else:
        print('Either --mo or --lora must be given as argument')


if __name__ == '__main__':
    cli()
