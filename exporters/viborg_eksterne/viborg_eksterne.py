# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

"""
Helper class to make a number of pre-defined queries into MO
"""
import datetime
import json
import logging
import pathlib
import time

from os2mo_helpers.mora_helpers import MoraHelper

cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
if not cfg_file.is_file():
    raise Exception('No setting file')
SETTINGS = json.loads(cfg_file.read_text())

logger = logging.getLogger(__name__)


def export_engagement(mh: MoraHelper, filename):
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
    employees = mh.read_all_users()

    logging.info('Reading engagements')
    for employee in employees:
        full_employee = mh.read_user(employee['uuid'])
        engagements = mh.read_user_engagement(
            employee['uuid'], read_all=True, skip_past=True
        )
        for eng in engagements:
            if eng['engagement_type']['uuid'] in disallowed_engagement_types:
                continue

            valid_from = datetime.datetime.strptime(eng['validity']['from'], '%Y-%m-%d')

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

    parent = mh.read_ou(org_unit_uuid).get('parent')
    if not parent:
        return {}
    return _find_manager(parent['uuid'], mora_helper)


def _find_manager_email(manager, mora_helper: MoraHelper):
    person_uuid = manager.get('person').get('uuid')

    email = mora_helper.get_e_address(person_uuid, "EMAIL").get('value')

    return email


if __name__ == '__main__':
    logger.info('Starting export')

    mora_base = SETTINGS['mora.base']
    query_exports_dir = pathlib.Path(SETTINGS["mora.folder.query_export"])
    outfile_name = query_exports_dir / SETTINGS[
        "exports_viborg_eksterne.outfile_basename"
    ]
    logger.info("writing to file %s", outfile_name)

    t = time.time()
    mh = MoraHelper(hostname=mora_base, export_ansi=False)

    export_engagement(mh, str(outfile_name))
    logger.info('Time: {}s'.format(time.time() - t))

    logger.info('Export completed')
