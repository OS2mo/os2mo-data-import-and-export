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
from anytree import PreOrderIter
from os2mo_helpers.mora_helpers import MoraHelper

from exporters.sql_export.lora_cache import LoraCache

LOG_LEVEL = logging.DEBUG
LOG_FILE = 'plan2learn.log'

logger = logging.getLogger('plan2learn')

for name in logging.root.manager.loggerDict:
    if name in ('LoraCache',  'mora-helper'):
        logging.getLogger(name).setLevel(LOG_LEVEL)
    else:
        logging.getLogger(name).setLevel(logging.ERROR)

logging.basicConfig(
    format='%(levelname)s %(asctime)s %(name)s %(message)s',
    level=LOG_LEVEL,
    filename=LOG_FILE
)


cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
if not cfg_file.is_file():
    raise Exception('No setting file')
SETTINGS = json.loads(cfg_file.read_text())

ACTIVE_JOB_FUNCTIONS = []  # Liste over aktive engagementer som skal eksporteres.


def export_bruger(mh, nodes, lc_historic):
    #  fieldnames = ['BrugerId', 'CPR', 'Navn', 'E-mail', 'Mobil', 'Stilling']
    used_cprs = []

    # Todo: Move to settings
    phone_type = '7db54183-1f2c-87ba-d4c3-de22a101ebc1'
    allowed_engagement_types = ['d3ffdf48-0ea2-72dc-6319-8597bdaa81d3',
                                'ac485d1c-025f-9818-f2c9-fafea2c1d282']

    rows = []
    for node in PreOrderIter(nodes['root']):
        if lc_historic:
            # TODO: If this is to run faster, we need to pre-sort into units,
            # to avoid iterating all engagements for each unit.
            for eng in lc_historic.engagements.values():
                for engv in eng:  # Iterate over all validities
                    if engv['unit'] != node.name:
                        continue
                    # if not engv['to_date']:
                    #     engv['to_date'] = '9999-12-31'
                    # valid_to = datetime.datetime.strptime(
                    #     engv['to_date'], '%Y-%m-%d'
                    # )
                    # if valid_to < datetime.datetime.now():
                    #     # Skip historic entries
                    #     continue
                    # Now, this is current or future validity
                    # This is a valid row from this unit
                    if engv['engagement_type'] not in allowed_engagement_types:
                        continue

                    user_uuid = engv['user']
                    user = lc_historic.users[user_uuid]
                    name = user['navn']
                    cpr = user['cpr']
                    if cpr in used_cprs:
                        # print('Skipping user: {} '.format(uuid))
                        continue

                    address = {}
                    for raw_address in lc_historic.addresses.values():
                        for addr_validity in raw_address:
                            if addr_validity['user'] == engv['user']:
                                if addr_validity['scope'] == 'EMAIL':
                                    address['E-mail'] = addr_validity['value']
                                if addr_validity['scope'] == 'PHONE':
                                    address['Telefon'] = addr_validity['value']

                    if cpr in used_cprs:
                        # print('Skipping user: {} '.format(uuid))
                        continue

                    used_cprs.append(cpr)
                    row = {
                        'BrugerId': user_uuid,
                        'CPR': cpr,
                        'Navn': name,
                        'E-mail': address.get('E-mail', ''),
                        'Mobil': address.get('Telefon', ''),
                        'Stilling': None  # To be populated later
                    }
                    rows.append(row)

        else:
            employees = mh.read_organisation_people(node.name, split_name=False,
                                                    read_all=True, skip_past=True)

            for uuid, employee in employees.items():
                if employee['engagement_type_uuid'] not in allowed_engagement_types:
                    continue
                address = mh.read_user_address(uuid, cpr=True, phone_type=phone_type)
                user_uuid = employee['Person UUID']
                name = employee['Navn']
                cpr = address['CPR-Nummer']
                if cpr in used_cprs:
                    # print('Skipping user: {} '.format(uuid))
                    continue

                used_cprs.append(cpr)
                row = {
                    'BrugerId': user_uuid,
                    'CPR': cpr,
                    'Navn': name,
                    'E-mail': address.get('E-mail', ''),
                    'Mobil': address.get('Telefon', ''),
                    'Stilling': None  # To be populated later
                }
                rows.append(row)
    # Turns out, we need to update this once we reach engagements
    # mh._write_csv(fieldnames, rows, filename)
    return rows


def _split_dar(address):
    if address:
        gade = address.split(',')[0]
        post = address.split(',')[1][1:5]
        by = address.split(',')[1][6:]
    else:
        gade = ''
        post = ''
        by = ''
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
        'exporters.plan2learn.allowed_engagement_types']

    rows = []

    # Keep a list of exported engagements to avoid exporting the same engagment
    # multiple times if it has multiple rows in MO.
    exported_engagements = []

    err_msg = 'Skipping {}, due to non-allowed engagement type'
    if lc and lc_historic:
        for employee in lc_historic.users.values():
            for eng in lc_historic.engagements.values():
                # We can consistenly access index 0, the historic export
                # is for the purpose of catching future engagements, not
                # to catch all validities
                engv = eng[0]
                if engv['user'] != employee['uuid']:
                    continue

                if engv['unit'] not in eksporterede_afdelinger:
                    logger.info('Unit {} is not included in the export')
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
                    # Todo, this we need to actually read from MO, no primary
                    # information available in historic cache.
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

                row = {
                    'EngagementId': engv['uuid'],
                    'BrugerId':  employee['uuid'],
                    'AfdelingsId': engv['unit'],
                    'AktivStatus': aktiv_status,
                    'StillingskodeId': stilingskode_id,
                    'Primær': primær,
                    'Engagementstype': engv['engagement_type'],
                    'StartdatoEngagement': start_dato
                }
                rows.append(row)
    else:
        employees = mh.read_all_users()
        for employee in employees:
            engagements = mh.read_user_engagement(employee['uuid'], read_all=True,
                                                  skip_past=True,
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

                valid_from = datetime.datetime.strptime(
                    eng['validity']['from'], '%Y-%m-%d'
                )
                active = valid_from < datetime.datetime.now()
                if active:
                    aktiv_status = 1
                    start_dato = ''
                else:
                    # Currently we always set engagment to active, even if it is not.
                    aktiv_status = 1
                    start_dato = eng['validity']['from']

                if eng['is_primary']:
                    primær = 1
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


if __name__ == '__main__':
    t = time.time()

    mh = MoraHelper(hostname=SETTINGS['mora.base'], export_ansi=False)

    dest_folder = pathlib.Path(SETTINGS['mora.folder.query_export'])

    # root_unit = '35840e9c-4480-4300-8000-000006140002'  # Short test
    root_unit = SETTINGS['exporters.plan2learn.root_unit']

    speed_up = True
    if speed_up:
        # Here we should activate read-only mode, actual state and
        # full history dumps needs to be in sync.

        # Full history does not calculate derived data, we must
        # fetch both kinds.
        lc = LoraCache(resolve_dar=True, full_history=False)
        lc.populate_cache(dry_run=True, skip_associations=True)
        lc.calculate_derived_unit_data()
        lc.calculate_primary_engagements()

        # Todo, in principle it should be possible to run with skip_past True
        lc_historic = LoraCache(resolve_dar=False, full_history=True,
                                skip_past=True)
        lc_historic.populate_cache(dry_run=True, skip_associations=True)
        # Here we should de-activate read-only mode

    # Todo: We need the nodes structure to keep a consistent output,
    # consider if the 70 seconds is worth the implementation.
    import pickle  # Development hack, remove before merge
    nodes_file = 'tmp/nodes.p'
    with open(nodes_file, 'rb') as f:
        nodes = pickle.load(f)
    # nodes = mh.read_ou_tree(root_unit)
    # print('Read nodes: {}s'.format(time.time() - t))
    # with open(nodes_file, 'wb') as f:
    #    pickle.dump(nodes, f, pickle.HIGHEST_PROTOCOL)

    brugere_rows = export_bruger(mh, nodes, lc_historic)
    print('Bruger: {}s'.format(time.time() - t))

    filename = str(dest_folder / 'plan2learn_organisation.csv')
    eksporterede_afdelinger = export_organisation(mh, nodes, filename, lc)
    print('Organisation: {}s'.format(time.time() - t))

    filename = str(dest_folder / 'plan2learn_engagement.csv')
    brugere_rows = export_engagement(mh, filename, eksporterede_afdelinger,
                                     brugere_rows, lc, lc_historic)
    print('Engagement: {}s'.format(time.time() - t))

    filename = str(dest_folder / 'plan2learn_stillingskode.csv')
    export_stillingskode(mh, nodes, filename)
    print('Stillingskode: {}s'.format(time.time() - t))

    filename = str(dest_folder / 'plan2learn_leder.csv')
    export_leder(mh, nodes, filename, eksporterede_afdelinger)
    print('Leder: {}s'.format(time.time() - t))

    # Now exported the now fully populated brugere.csv
    filename = str(dest_folder / 'plan2learn_bruger.csv')
    brugere_fieldnames = ['BrugerId', 'CPR', 'Navn', 'E-mail', 'Mobil', 'Stilling']
    mh._write_csv(brugere_fieldnames, brugere_rows, filename)

    print('Export completed')
