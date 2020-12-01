# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

"""
Helper class to make a number of pre-defined queries into MO
"""
import functools

import time
import json
import logging
import pathlib
import argparse
import datetime

from os2mo_helpers.mora_helpers import MoraHelper
from exporters.sql_export.lora_cache import LoraCache
from tools.priority_by_class import lc_choose_public_address


LOG_LEVEL = logging.DEBUG
LOG_FILE = 'viborg_externe.log'

logger = logging.getLogger('viborg_externe')

for name in logging.root.manager.loggerDict:
    if name in ('LoraCache',  'mora-helper', 'viborg_externe'):
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


def export_engagement(mh: MoraHelper, filename, lc, lc_historic):
    fieldnames = [
        'OrganisationsenhedUUID',
        'Organisationsenhed',
        'Ledernavn',
        'Lederemail',
        'Tjenestenummer',
        'CPR-nummer',
        'Navn',
        'Engagementstype',
        'Startdato',
    ]

    # Medarbejder (månedsløn) and Medarbejder (timeløn)
    disallowed_engagement_types = SETTINGS[
        'exporters.plan2learn.allowed_engagement_types'
    ]
    rows = []

    logging.info('Reading users')
    if lc:
        employees = list(map(lambda x: x[0], lc.users.values()))
    else:
        employees = mh.read_all_users()

    logging.info('Reading engagements')
    # Todo: This O(#employees x #engagments), a pre-sorting of engagements would
    # make it O(#employees + #engagments) - consider if this is worth the effort
    for employee in employees:
        if lc:
            for eng in filter(
                lambda x: x[0]['user'] == employee['uuid'],
                lc_historic.engagements.values()
            ):
                engv = eng[0]  # Historic information is here to catch future
                # engagements, not to use the actual historic information
                #if engv['user'] != employee['uuid']:
                #    continue
                if engv['engagement_type'] in disallowed_engagement_types:
                    continue

                org_unit_uuid = engv['unit']
                org_unit_name = lc.units[org_unit_uuid][0]['name']
                manager = lc.units[org_unit_uuid][0]['acting_manager_uuid']

                if manager:
                    manager_object = lc_historic.managers[manager][0]
                    manager_name = lc.users[manager_object['user']][0]['navn']
                    manager_email = ""

                    manager_email_candidates = [x[0] for x in filter(
                        lambda x: (
                            x[0]['scope'] == 'E-mail' and
                            x[0]['user'] == manager_object['user']
                        ),
                        lc_historic.addresses.values()
                    )]

                    chosen = lc_choose_public_address(
                        manager_email_candidates,
                        SETTINGS.get("exports_viborg_eksterne.email.priority", []),
                        lc # for class lookup
                    )

                    if chosen:
                        manager_email = chosen['value']
                else:
                    logger.warning(
                        "No manager found for org unit: {}".format(org_unit_uuid)
                    )
                    manager_name = ""
                    manager_email = ""

                engagement_type = lc.classes[engv['engagement_type']]['title']
                row = {
                    'OrganisationsenhedUUID': org_unit_uuid,
                    'Organisationsenhed': org_unit_name,
                    'Ledernavn': manager_name,
                    'Lederemail': manager_email,
                    'Tjenestenummer': engv['user_key'],
                    'CPR-nummer': employee['cpr'],
                    'Navn': employee['navn'],
                    'Engagementstype': engagement_type,
                    # 'Startdato': valid_from.strftime('%Y-%m-%d') + " 00:00:00",
                    'Startdato': engv['from_date'] + " 00:00:00"
                }
                rows.append(row)
        else:
            full_employee = mh.read_user(employee['uuid'])
            engagements = mh.read_user_engagement(
                employee['uuid'], read_all=True, skip_past=True
            )
            for eng in engagements:
                if eng['engagement_type']['uuid'] in disallowed_engagement_types:
                    continue

                valid_from = datetime.datetime.strptime(eng['validity']['from'],
                                                        '%Y-%m-%d')

                org_unit_uuid = eng['org_unit']['uuid']
                manager = _find_manager(org_unit_uuid, mh)
                if manager:
                    manager_name = manager['person']['name']
                    manager_email = _find_manager_email(manager, mh)
                else:
                    logger.warning(
                        "No manager found for org unit: {}".format(org_unit_uuid)
                    )
                    manager_name = ""
                    manager_email = ""

                row = {
                    'OrganisationsenhedUUID': org_unit_uuid,
                    'Organisationsenhed': eng['org_unit']['name'],
                    'Ledernavn': manager_name,
                    'Lederemail': manager_email,
                    'Tjenestenummer': eng['user_key'],
                    'CPR-nummer': full_employee['cpr_no'],
                    'Navn': full_employee['name'],
                    'Engagementstype': eng['engagement_type']['name'],
                    'Startdato': valid_from,
                }
                rows.append(row)

    mh._write_csv(fieldnames, rows, filename)


def _find_manager(org_unit_uuid, mora_helper: MoraHelper):
    url = "ou/{}/details/manager"

    managers = mora_helper._mo_lookup(org_unit_uuid, url)

    responsibility_class = SETTINGS['exporters.viborg.primary_manager_responsibility']

    for manager in managers:
        if responsibility_class in map(
            lambda x: x.get('uuid'), manager['responsibility']
        ):
            return manager

    parent = mora_helper.read_ou(org_unit_uuid).get('parent')
    if not parent:
        return {}
    return _find_manager(parent['uuid'], mora_helper)


def _find_manager_email(manager, mora_helper: MoraHelper):
    person_uuid = manager.get('person').get('uuid')

    email = mora_helper.get_e_address(person_uuid, "EMAIL").get('value')

    return email


def main(speedup, dry_run=False):
    logger.info('Starting export')

    mora_base = SETTINGS['mora.base']
    query_exports_dir = pathlib.Path(SETTINGS["mora.folder.query_export"])
    if 'exports_viborg_eksterne.outfile_basename' not in SETTINGS:
        print('Missing key in settings: exports_viborg_eksterne.outfile_basename')
        exit(1)
    outfile_name = query_exports_dir / SETTINGS[
        "exports_viborg_eksterne.outfile_basename"
    ]
    logger.info("writing to file %s", outfile_name)

    t = time.time()
    mh = MoraHelper(hostname=mora_base, export_ansi=False)

    if speedup:
        # Here we should activate read-only mode, actual state and
        # full history dumps needs to be in sync.

        # Full history does not calculate derived data, we must
        # fetch both kinds.
        lc = LoraCache(resolve_dar=True, full_history=False)
        lc.populate_cache(dry_run=dry_run, skip_associations=True)
        lc.calculate_derived_unit_data()

        lc_historic = LoraCache(resolve_dar=False, full_history=True,
                                skip_past=True)
        lc_historic.populate_cache(dry_run=dry_run, skip_associations=True)
        # Here we should de-activate read-only mode
    else:
        lc = None
        lc_historic = None

    export_engagement(mh, str(outfile_name), lc, lc_historic)
    logger.info('Time: {}s'.format(time.time() - t))

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
