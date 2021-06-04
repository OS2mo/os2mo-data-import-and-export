import time
import json
import pathlib
import logging
import datetime

import click
from click_option_group import optgroup, RequiredMutuallyExclusiveOptionGroup

# SD?
# from integrations.SD_Lon import sd_payloads
from integrations.opus import payloads
from os2mo_helpers.mora_helpers import MoraHelper
from ra_utils.load_settings import load_settings


SETTINGS = load_settings()
MORA_BASE = SETTINGS['mora.base']

logger = logging.getLogger("updatePrimaryEngagements")
LOG_LEVEL = logging.DEBUG
LOG_FILE = 'calculate_primary.log'


class MOPrimaryEngagementUpdater(object):
    def __init__(self):
        self.helper = MoraHelper(hostname=MORA_BASE, use_cache=False)
        self.org_uuid = self.helper.read_organisation()

        self.mo_person = None

        # Currently primary is set first by engagement type (order given in
        # settings) and secondly by job_id. self.primary is an ordered list of
        # classes that can considered to be primary. self.primary_types is a dict
        # with all classes in the primary facet.
        self.eng_types_order = SETTINGS['integrations.opus.eng_types_primary_order']
        self.primary_types, self.primary = self._find_primary_types()

    def _find_primary_types(self):
        """
        Read the engagement types from MO and match them up against the three
        known types in the OPUS->MO import.
        :param helper: An instance of mora-helpers.
        :return: A dict matching up the engagement types with LoRa class uuids.
        """
        # These constants are global in all OPUS municipalities (because they are
        # created by the OPUS->MO importer.
        PRIMARY = 'primary'
        NON_PRIMARY = 'non-primary'
        FIXED_PRIMARY = 'explicitly-primary'

        logger.info('Read primary types')
        primary_dict = {
            'fixed_primary': None,
            'primary': None,
            'non_primary': None
        }

        primary_types = self.helper.read_classes_in_facet('primary_type')
        for primary_type in primary_types[0]:
            if primary_type['user_key'] == PRIMARY:
                primary_dict['primary'] = primary_type['uuid']
            if primary_type['user_key'] == NON_PRIMARY:
                primary_dict['non_primary'] = primary_type['uuid']
            if primary_type['user_key'] == FIXED_PRIMARY:
                primary_dict['fixed_primary'] = primary_type['uuid']

        if None in primary_dict.values():
            raise Exception('Missing primary types: {}'.format(primary_dict))
        primary_list = [primary_dict['fixed_primary'], primary_dict['primary']]

        return primary_dict, primary_list

    def set_current_person(self, cpr=None, uuid=None, mo_person=None):
        """
        Set a new person as the current user. Either a cpr-number or
        an uuid should be given, not both.
        :param cpr: cpr number of the person.
        :param uuid: MO uuid of the person.
        :param mo_person: An already existing user object from mora_helper.
        :return: True if current user is valid, otherwise False.
        """
        if uuid:
            mo_person = self.helper.read_user(user_uuid=uuid)
        elif cpr:
            mo_person = self.helper.read_user(user_cpr=cpr, org_uuid=self.org_uuid)
        elif mo_person:
            pass
        else:
            mo_person = None
        # print('Read user: {}s'.format(time.time() - t))
        if mo_person:
            self.mo_person = mo_person
            success = True
        else:
            self.mo_person = None
            success = False
        return success

    def _engagements_included_in_primary_calculation(self, engagements):
        included = []
        for eng in engagements:
            # disregard engagements from externals
            if eng["org_unit"]["uuid"] in SETTINGS.get("integrations.ad.import_ou.mo_unit_uuid",""):
                logger.warning('disregarding external engagement: {}'.format(eng))
                continue
            included.append(eng)
        return included

    def _calculate_rate_and_ids(self, mo_engagement):
        min_type_pri = 9999
        min_id = 9999999
        for eng in mo_engagement:
            logger.debug('Calculate rate, engagement: {}'.format(eng))

            try:
                employment_id = int(eng['user_key'])
            except ValueError:
                logger.warning("Skippning engangement with non-integer employment_id: {}".format(eng['user_key']))
                continue

            stat = 'Current eng_type, min_id: {}, {}. This rate, eng_pos: {}, {}'
            logger.debug(stat.format(min_type_pri, min_id,
                                     employment_id, eng['fraction']))

            if eng['engagement_type'] in self.eng_types_order:
                type_pri = self.eng_types_order.index(eng['engagement_type'])
            else:
                type_pri = 9999

            if type_pri == min_type_pri:
                if employment_id < min_id:
                    min_id = employment_id
            if type_pri < min_type_pri:
                min_id = employment_id
                min_type_pri = type_pri

        logger.debug('Min id: {}, Prioritied type: {}'.format(min_id, min_type_pri))
        return (min_id, min_type_pri)

    def check_all_for_primary(self):
        """
        Check all users for the existence of primary engagements.
        :return: TODO
        """
        count = 0
        all_users = self.helper.read_all_users()
        for user in all_users:
            if count % 250 == 0:
                print('{}/{}'.format(count, len(all_users)))
            count += 1

            self.set_current_person(uuid=user['uuid'])
            date_list = self.helper.find_cut_dates(user['uuid'])
            for i in range(0, len(date_list) - 1):
                date = date_list[i]
                mo_engagement = self.helper.read_user_engagement(
                    user=self.mo_person['uuid'], at=date, only_primary=True
                )

                primary_count = 0
                for eng in mo_engagement:
                    if eng['primary']['uuid'] in self.primary:
                        primary_count += 1
            if primary_count == 0:
                print('No primary for {} at {}'.format(user['uuid'], date))
            elif primary_count > 1:
                print('Too many primaries for {} at {}'.format(user['uuid'], date))
            else:
                # print('Correct')
                pass

    def recalculate_primary(self, no_past=True):
        """
        Re-calculate primary engagement for the entire history of the current user.
        """
        logger.info('Calculate primary engagement: {}'.format(self.mo_person))
        date_list = self.helper.find_cut_dates(self.mo_person['uuid'],
                                               no_past=no_past)

        number_of_edits = 0

        for i in range(0, len(date_list) - 1):
            date = date_list[i]
            logger.info('Recalculate primary, date: {}'.format(date))

            mo_engagement = self.helper.read_user_engagement(
                user=self.mo_person['uuid'], at=date, only_primary=True, use_cache=False
            )

            mo_engagement = self._engagements_included_in_primary_calculation(mo_engagement)
            if len(mo_engagement) == 0:
                continue

            logger.debug('MO engagement: {}'.format(mo_engagement))

            (min_id, min_type_pri) = self._calculate_rate_and_ids(mo_engagement)
            if (min_id is None) or (min_type_pri is None):
                # continue
                raise Exception('Cannot calculate primary')

            fixed = None
            for eng in mo_engagement:
                if eng['primary']:
                    if eng['primary']['uuid'] == self.primary_types['fixed_primary']:
                        logger.info('Engagment {} is fixed primary'.format(eng['uuid']))
                        fixed = eng['uuid']

            exactly_one_primary = False
            for eng in mo_engagement:
                to = datetime.datetime.strftime(
                    date_list[i + 1] - datetime.timedelta(days=1), "%Y-%m-%d"
                )
                if date_list[i + 1] == datetime.datetime(9999, 12, 30, 0, 0):
                    to = None
                validity = {
                    'from': datetime.datetime.strftime(date, "%Y-%m-%d"),
                    'to': to
                }

                try:
                    employment_id = int(eng['user_key'])
                except ValueError:
                    logger.warning("Skippning engangement with non-integer employment_id: {}".format(eng['user_key']))
                    continue
                if eng['engagement_type'] in self.eng_types_order:
                    type_pri = self.eng_types_order.index(eng['engagement_type'])
                else:
                    type_pri = 9999

                msg = 'Current type pri and id: {}, {}'
                logger.debug(msg.format(type_pri, employment_id))

                if type_pri == min_type_pri and employment_id == min_id:
                    assert(exactly_one_primary is False)
                    logger.debug('Primary is: {}'.format(employment_id))
                    exactly_one_primary = True
                    current_type = self.primary_types['primary']
                else:
                    logger.debug('{} is not primary'.format(employment_id))
                    current_type = self.primary_types['non_primary']

                if fixed is not None and eng['uuid'] != fixed:
                    # A fixed primary exits, but this is not it.
                    logger.debug('Manual override, this is not primary!')
                    current_type = self.primary_types['non_primary']
                if eng['uuid'] == fixed:
                    # This is a fixed primary.
                    current_type = self.primary_types['fixed_primary']

                data = {
                    'primary': {'uuid': current_type},
                    'validity': validity
                }

                payload = payloads.edit_engagement(data, eng['uuid'])
                if not payload['data']['primary'] == eng['primary']:
                    logger.debug('Edit payload: {}'.format(payload))
                    response = self.helper._mo_post('details/edit', payload)
                    assert response.status_code == 200
                    number_of_edits += 1
                else:
                    logger.debug('No edit, primary type not changed.')
        return_dict = {self.mo_person['uuid']: number_of_edits}
        return return_dict

    def recalculate_all(self, no_past=False):
        """
        Recalculate all primary engagements
        :return: TODO
        """
        all_users = self.helper.read_all_users()
        edit_status = {}
        for user in all_users:
            t = time.time()
            self.set_current_person(uuid=user['uuid'])
            status = self.recalculate_primary(no_past=no_past)
            edit_status.update(status)
            logger.debug('Time for primary calculation: {}'.format(time.time() - t))
        print('Total edits: {}'.format(sum(edit_status.values())))


@click.command(help="Calculate Primary")
@optgroup.group("Action", cls=RequiredMutuallyExclusiveOptionGroup)
@optgroup.option(
    "--check-all-for-primary",
    is_flag=True,
    help='Check all users for a primary engagement',
)
@optgroup.option("--recalculate-all", is_flag=True, help='Recalculate all users')
@optgroup.option(
    "--recalculate-user",
    help='Recalculate primaries for a user (specify MO UUID)',
)
def cli(**args):
    updater = MOPrimaryEngagementUpdater()

    if args['recalculate_user']:
        print('Recalculate user')
        t = time.time()
        updater.set_current_person(uuid=args['recalculate_user'])
        updater.recalculate_primary()
        print('Time for primary calculation: {}'.format(time.time() - t))

    if args['check_all_for_primary']:
        print('Check all for primary')
        updater.check_all_for_primary()

    if args['recalculate_all']:
        print('Check all for primary')
        updater.recalculate_all(no_past=True)


if __name__ == '__main__':
    detail_logging = ('mora-helper', 'updatePrimaryEngagements', 'sdCommon')
    for name in logging.root.manager.loggerDict:
        if name in detail_logging:
            logging.getLogger(name).setLevel(LOG_LEVEL)
        else:
            logging.getLogger(name).setLevel(logging.ERROR)

    logging.basicConfig(
        format='%(levelname)s %(asctime)s %(name)s %(message)s',
        level=LOG_LEVEL,
        filename=LOG_FILE
    )

    cli()