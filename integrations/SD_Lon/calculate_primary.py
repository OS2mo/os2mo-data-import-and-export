import os
import logging
import argparse
import datetime

import sd_common
import sd_payloads

from os2mo_helpers.mora_helpers import MoraHelper

MORA_BASE = os.environ.get('MORA_BASE', None)

logger = logging.getLogger("updatePrimaryEngagements")
LOG_LEVEL = logging.DEBUG
LOG_FILE = 'calculate_primary.log'


class MOPrimaryEngagementUpdater(object):
    def __init__(self):
        self.helper = MoraHelper(hostname=MORA_BASE, use_cache=False)
        self.org_uuid = self.helper.read_organisation()

        self.mo_person = None

        # Keys are; fixed_primary, primary no_sallery non-primary
        self.eng_types = sd_common.engagement_types(self.helper)
        self.primary = [
            self.eng_types['fixed_primary'],
            self.eng_types['primary'],
            self.eng_types['no_sallery']
        ]

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

        if mo_person:
            self.mo_person = mo_person
            success = True
        else:
            self.mo_person = None
            success = False
        return success

    def _calculate_rate_and_ids(self, mo_engagement):
        max_rate = 0
        min_id = 9999999
        for eng in mo_engagement:
            logger.debug('Calculate rate, engagement: {}'.format(eng))
            if 'user_key' not in eng:
                logger.error('Cannot calculate primary!!! Eng: {}'.format(eng))
                return None, None

            try:  # Code similar to this exists in common.
                employment_id = int(eng['user_key'])
            except ValueError:
                employment_id = 999999

            if not eng['fraction']:
                eng['fraction'] = 0

            stat = 'Cur max rate: {}, cur min_id: {}, this rate: {}, this id: {}'
            logger.debug(stat.format(max_rate, min_id,
                                     employment_id, eng['fraction']))

            occupation_rate = eng['fraction']
            if eng['fraction'] == max_rate:
                if employment_id < min_id:
                    min_id = employment_id
            if occupation_rate > max_rate:
                max_rate = occupation_rate
                min_id = employment_id
        logger.debug('Min id: {}, Max rate: {}'.format(min_id, max_rate))
        return (min_id, max_rate)

    def _find_cut_dates(self):
        """
        Run throgh entire history of current user and return a list of dates with
        changes in the engagement.
        """
        uuid = self.mo_person['uuid']
        mo_engagement = self.helper.read_user_engagement(
            user=uuid,
            only_primary=True,
            read_all=True,
        )
        dates = set()
        for eng in mo_engagement:
            dates.add(datetime.datetime.strptime(eng['validity']['from'],
                                                 '%Y-%m-%d'))
            if eng['validity']['to']:
                to = datetime.datetime.strptime(eng['validity']['to'], '%Y-%m-%d')
                day_after = to + datetime.timedelta(days=1)
                dates.add(day_after)
            else:
                dates.add(datetime.datetime(9999, 12, 30, 0, 0))

        date_list = sorted(list(dates))
        logger.debug('List of cut-dates: {}'.format(date_list))
        return date_list

    def _read_engagement(self, date):
        mo_engagement = self.helper.read_user_engagement(
            user=self.mo_person['uuid'],
            at=date,
            only_primary=True,  # Do not read extended info from MO.
            use_cache=False
        )
        return mo_engagement

    def check_all_for_primary(self):
        """
        Check all users for the existence of primary engagements.
        :return: TODO
        """
        # TODO: This is a seperate function in AD Sync! Change to mora_helpers!
        all_users = self.helper.read_all_users()
        for user in all_users:
            self.set_current_person(uuid=user['uuid'])
            date_list = self._find_cut_dates()
            for i in range(0, len(date_list) - 1):
                date = date_list[i]
                mo_engagement = self._read_engagement(date)
                primary_count = 0
                for eng in mo_engagement:
                    if eng['engagement_type']['uuid'] in self.primary:
                        primary_count += 1
            if primary_count == 0:
                print('No primary for {} at {}'.format(user['uuid'], date))
            elif primary_count > 1:
                # This will typically happen because of both a primary and a status0
                logger.info('{} has more than one primary'.format(user['uuid']))
                extra_primary_count = 0
                for eng in mo_engagement:
                    if eng['engagement_type']['uuid'] in self.primary[0:2]:
                        extra_primary_count += 1
                if extra_primary_count == 1:
                    logger.info('Only one primary was different from status 0')
                if extra_primary_count > 1:
                    print('Too many primaries for {} at {}'.format(user['uuid'],
                                                                   date))
            else:
                # print('Correct')
                pass

    def recalculate_primary(self):
        """
        Re-calculate primary engagement for the enire history of the current user.
        """
        logger.info('Calculate primary engagement: {}'.format(self.mo_person))
        date_list = self._find_cut_dates()
        number_of_edits = 0

        for i in range(0, len(date_list) - 1):
            date = date_list[i]

            mo_engagement = self._read_engagement(date)
            logger.debug('MO engagement: {}'.format(mo_engagement))
            (min_id, max_rate) = self._calculate_rate_and_ids(mo_engagement)
            if (min_id is None) or (max_rate is None):
                continue

            fixed = None
            for eng in mo_engagement:
                if eng['engagement_type']['uuid'] == self.eng_types['fixed_primary']:
                    logger.info('Engagment {} is fixed primary'.format(eng['uuid']))
                    fixed = eng['uuid']

            exactly_one_primary = False
            for eng in mo_engagement:
                if eng['engagement_type']['uuid'] == self.eng_types['no_sallery']:
                    logger.info('Status 0, no update of primary')
                    continue

                if date_list[i + 1] == datetime.datetime(9999, 12, 30, 0, 0):
                    to = None
                else:
                    to = datetime.datetime.strftime(
                        date_list[i + 1] - datetime.timedelta(days=1), "%Y-%m-%d"
                    )
                validity = {
                    'from': datetime.datetime.strftime(date, "%Y-%m-%d"),
                    'to': to
                }

                if 'user_key' not in eng:
                    break
                try:
                    # non-integer user keys should universially be status0
                    employment_id = int(eng['user_key'])
                except ValueError:
                    logger.warning('Engagement type not status0?')
                    employment_id = 9999999
                    eng['fraction'] = 0

                occupation_rate = 0
                if eng['fraction']:
                    occupation_rate = eng['fraction']

                logger.debug('Current rate and id: {}, {}'.format(occupation_rate,
                                                                  employment_id))

                # We explicit do not set the MO primary field, since this
                # would need to be manually synchronized in case of manual
                # changes from the front-end.
                if occupation_rate == max_rate and employment_id == min_id:
                    assert(exactly_one_primary is False)
                    logger.debug('Primary is: {}'.format(employment_id))
                    exactly_one_primary = True
                    current_type = self.eng_types['primary']
                else:
                    logger.debug('{} is not primary'.format(employment_id))
                    current_type = self.eng_types['non_primary']

                if fixed is not None and eng['uuid'] != fixed:
                    # A fixed primary exits, but this is not it.
                    logger.debug('Manual override, this is not primary!')
                    current_type = self.eng_types['non_primary']
                if eng['uuid'] == fixed:
                    # This is a fixed primary.
                    current_type = self.eng_types['fixed_primary']

                data = {
                    'engagement_type': {'uuid': current_type},
                    'validity': validity
                }

                payload = sd_payloads.engagement(data, eng)
                if not payload['data']['engagement_type'] == eng['engagement_type']:
                    logger.debug('Edit payload: {}'.format(payload))
                    response = self.helper._mo_post('details/edit', payload)
                    assert response.status_code == 200
                    number_of_edits += 1
                else:
                    logger.debug('No edit, engagement type not changed.')
        return_dict = {self.mo_person['uuid']: number_of_edits}
        return return_dict

    def recalculate_all(self):
        """
        Recalculate all primary engagements
        :return: TODO
        """
        all_users = self.helper.read_all_users()
        edit_status = {}
        for user in all_users:
            self.set_current_person(uuid=user['uuid'])
            status = self.recalculate_primary()
            edit_status.update(status)
        print('Total edits: {}'.format(sum(edit_status.values())))

    def _cli(self):
        parser = argparse.ArgumentParser(description='Calculate Primary')
        group = parser.add_mutually_exclusive_group()
        group.add_argument('--check_all_for_primary', nargs=1,
                           help='Check all users for a primary engagement')
        group.add_argument('--recalculate_all', nargs=1,
                           help='Recalculate all all users')
        group.add_argument('--recalculate_user', nargs=1, metavar='MO uuid',
                           help='Recalculate primaries for a user')

        args = vars(parser.parse_args())

        if args.get('recalculate_user'):
            print('Recalculate user')
            uuid = args.get('recalculate_user')[0]
            self.set_current_person(uuid=uuid)
            self.recalculate_primary()

        if args.get('check_all_for_primary'):
            print('Check all for primary')
            self.check_all_for_primary()

        if args.get('recalculate_all'):
            print('Check all for primary')
            self.recalculate_all()


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

    #  FINISH CHANGE_AT TO USE THIS MODULE

    updater = MOPrimaryEngagementUpdater()
    updater._cli()
    # updater.recalculate_all()

    # cpr = ''
    # updater._set_current_person(cpr=cpr)
    # print(updater.recalculate_primary())

