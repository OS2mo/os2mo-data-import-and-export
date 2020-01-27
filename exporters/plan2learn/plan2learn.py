# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

"""
Helper class to make a number of pre-defined queries into MO
"""
import os
import json
import time
import pathlib
import datetime
from anytree import PreOrderIter
from os2mo_helpers.mora_helpers import MoraHelper

MORA_BASE = os.environ.get('MORA_BASE', 'http://localhost:5000')

cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
if not cfg_file.is_file():
    raise Exception('No setting file')
SETTINGS = json.loads(cfg_file.read_text())

ACTIVE_JOB_FUNCTIONS = []  # Liste over aktive engagementer som skal eksporteres.


def export_bruger(mh, nodes, filename):
    fieldnames = ['BrugerId', 'CPR', 'Navn', 'E-mail', 'Mobil']

    # Todo: Move to settings
    phone_type = '7db54183-1f2c-87ba-d4c3-de22a101ebc1'
    allowed_engagement_types = ['d3ffdf48-0ea2-72dc-6319-8597bdaa81d3',
                                'ac485d1c-025f-9818-f2c9-fafea2c1d282']

    rows = []
    for node in PreOrderIter(nodes['root']):
        employees = mh.read_organisation_people(node.name, split_name=False,
                                                read_all=True, skip_past=True)
        for uuid, employee in employees.items():
            if employee['engagement_type_uuid'] not in allowed_engagement_types:
                continue
            address = mh.read_user_address(uuid, cpr=True, phone_type=phone_type)
            row = {
                'BrugerId': employee['Person UUID'],
                'CPR': address['CPR-Nummer'],
                'Navn': employee['Navn'],
                'E-mail': address.get('E-mail', ''),
                'Mobil': address.get('Telefon', ''),
            }
            rows.append(row)
    mh._write_csv(fieldnames, rows, filename)


def export_organisation(mh, nodes, filename):
    fieldnames = ['AfdelingsID', 'Afdelingsnavn', 'Parentid', 'Gade', 'Postnr', 'By']

    rows = []
    for node in PreOrderIter(nodes['root']):
        ou = mh.read_ou(node.name)
        level = ou['org_unit_level']
        if level['name'] in SETTINGS['integrations.SD_Lon.import.too_deep']:
            continue

        over_uuid = ou['parent']['uuid'] if ou['parent'] else ''

        dar_address = mh.read_ou_address(node.name)
        if 'Adresse' in dar_address:
            gade = dar_address['Adresse'].split(',')[0]
            post = dar_address['Adresse'].split(',')[1][1:5]
            by = dar_address['Adresse'].split(',')[1][6:]
        else:
            gade = ''
            post = ''
            by = ''

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


def export_engagement(mh, nodes, filename):
    fieldnames = ['BrugerId', 'AfdelingsId', 'AktivStatus', 'StillingskodeId',
                  'Primær', 'Engagementstype', 'StartdatoEngagement']

    # Todo: Move to settings
    allowed_engagement_types = ['d3ffdf48-0ea2-72dc-6319-8597bdaa81d3',
                                'ac485d1c-025f-9818-f2c9-fafea2c1d282']

    rows = []
    # employees = mh.read_all_users(limit=10)
    employees = mh.read_all_users()
    for employee in employees:
        engagements = mh.read_user_engagement(employee['uuid'],
                                              calculate_primary=True)
        for eng in engagements:
            if eng['engagement_type']['uuid'] not in allowed_engagement_types:
                continue

            valid_from = datetime.datetime.strptime(
                eng['validity']['from'], '%Y-%m-%d'
            )
            active = valid_from < datetime.datetime.now()
            if active:
                aktiv_status = 1
                start_dato = ''
            else:
                aktiv_status = 0
                start_dato = eng['validity']['from']

            if eng['is_primary']:
                primær = 1
            else:
                primær = 0

            stilingskode_id = eng['job_function']['uuid']
            ACTIVE_JOB_FUNCTIONS.append(stilingskode_id)

            row = {
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


def export_stillingskode(mh, nodes, filename):
    fieldnames = ['StillingskodeID', 'AktivStatus', 'Stillingskode',
                  'Stillingskode#']
    stillinger = mh.read_classes_in_facet('engagement_job_function')

    rows = []
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


def export_leder(mh, nodes, filename):
    fieldnames = ['BrugerId', 'AfdelingsID', 'AktivStatus']
    rows = []
    for node in PreOrderIter(nodes['root']):
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

    mh = MoraHelper(hostname=MORA_BASE, export_ansi=False)

    # In real life, this should go to settings
    # root_unit = '35840e9c-4480-4300-8000-000006140002'  # Short test
    root_unit = '4f79e266-4080-4300-a800-000006180002'  # Full run

    nodes = mh.read_ou_tree(root_unit)
    print('Read nodes: {}s'.format(time.time() - t))

    filename = 'plan2lean_bruger.csv'
    export_bruger(mh, nodes, filename)
    print('Bruger: {}s'.format(time.time() - t))

    filename = 'plan2lean_organisation.csv'
    export_organisation(mh, nodes, filename)
    print('Organisation: {}s'.format(time.time() - t))

    filename = 'plan2lean_engagement.csv'
    export_engagement(mh, nodes, filename)
    print('Engagement: {}s'.format(time.time() - t))

    filename = 'plan2lean_stillingskode.csv'
    export_stillingskode(mh, nodes, filename)
    print('Stillingskode: {}s'.format(time.time() - t))

    filename = 'plan2lean_leder.csv'
    export_leder(mh, nodes, filename)
    print('Leder: {}s'.format(time.time() - t))

    print('Export completed')
