import time
import json
import pathlib
import logging
import argparse
import datetime
import asyncio

from operator import itemgetter


async def primary_types(helper):
    """
    Read the engagement types from MO and match them up against the four
    known types in the SD->MO import.
    :param helper: An instance of mora-helpers.
    :return: A dict matching up the engagement types with LoRa class uuids.
    """
    # These constants are global in all SD municipalities (because they are created
    # by the SD->MO importer.
    PRIMARY = 'primary'
    NO_SALARY = 'non-primary'
    NON_PRIMARY = 'non-primary'
    FIXED_PRIMARY = 'explicitly-primary'

    logger.info('Read primary types')
    primary = None
    no_salary = None
    non_primary = None
    fixed_primary = None

    primary_types = await helper.read_classes_in_facet('primary_type')
    for primary_type in primary_types[0]:
        if primary_type['user_key'] == PRIMARY:
            primary = primary_type['uuid']
        if primary_type['user_key'] == NON_PRIMARY:
            non_primary = primary_type['uuid']
        if primary_type['user_key'] == NO_SALARY:
            no_salary = primary_type['uuid']
        if primary_type['user_key'] == FIXED_PRIMARY:
            fixed_primary = primary_type['uuid']

    type_uuids = {
        'primary': primary,
        'non_primary': non_primary,
        'no_salary': no_salary,
        'fixed_primary': fixed_primary
    }
    if None in type_uuids.values():
        raise Exception('Missing primary types: {}'.format(type_uuids))
    return type_uuids


from integrations.SD_Lon import sd_payloads

from os2mo_helpers.mora_helpers import MoraHelper

# TODO: Soon we have done this 4 times. Should we make a small settings
# importer, that will also handle datatype for specicic keys?
cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
if not cfg_file.is_file():
    raise Exception('No setting file')
SETTINGS = json.loads(cfg_file.read_text())
MORA_BASE = SETTINGS['mora.base']

logger = logging.getLogger("updatePrimaryEngagements")
LOG_LEVEL = logging.DEBUG
LOG_FILE = 'calculate_primary.log'


class MOPrimaryEngagementUpdater(object):
    def __init__(self):
        self.helper = MoraHelper(hostname=MORA_BASE, use_cache=False)

    async def setup(self):
        self.org_uuid = await self.helper.read_organisation()

        # Keys are; fixed_primary, primary no_salary non-primary
        self.primary_types = await primary_types(self.helper)
        self.primary = [
            self.primary_types['fixed_primary'],
            self.primary_types['primary'],
            self.primary_types['no_salary']
        ]

    async def set_current_person(self, cpr=None, uuid=None, mo_person=None):
        """
        Set a new person as the current user. Either a cpr-number or
        an uuid should be given, not both.
        :param cpr: cpr number of the person.
        :param uuid: MO uuid of the person.
        :param mo_person: An already existing user object from mora_helper.
        :return: True if current user is valid, otherwise False.
        """
        if uuid:
            mo_person = await self.helper.read_user(user_uuid=uuid)
        elif cpr:
            mo_person = await self.helper.read_user(user_cpr=cpr, org_uuid=self.org_uuid)
        elif mo_person:
            pass
        else:
            mo_person = None
        # print('Read user: {}s'.format(time.time() - t))
        if mo_person:
            return True, mo_person
        else:
            return False, None

    def _calculate_rate_and_ids(self, mo_engagement, no_past):
        max_rate = 0
        min_id = 9999999
        for eng in mo_engagement:
            if no_past and eng['validity']['to']:
                to = datetime.datetime.strptime(
                    eng['validity']['to'], '%Y-%m-%d')
                if to < datetime.datetime.now():
                    continue

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

    async def _find_cut_dates(self, no_past=False, mo_person=None):
        """
        Run throgh entire history of current user and return a list of dates with
        changes in the engagement.
        """
        uuid = mo_person['uuid']

        mo_engagement = await self.helper.read_user_engagement(
            user=uuid,
            only_primary=True,
            read_all=True,
            skip_past=no_past
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
        # print('Find cut dates: {}s'.format(time.time() - t))
        return date_list

    async def _read_engagement(self, date, mo_person=None):
        mo_engagement = await self.helper.read_user_engagement(
            user=mo_person['uuid'],
            at=str(date),
            only_primary=True,  # Do not read extended info from MO.
            use_cache=False
        )
        return mo_engagement

    async def check_all_for_primary(self):
        """
        Check all users for the existence of primary engagements.
        :return: TODO
        """
        # TODO: This is a seperate function in AD Sync! Change to mora_helpers!
        count = 0
        all_users = await self.helper.read_all_users()
        for user in all_users:
            if count % 250 == 0:
                print('{}/{}'.format(count, len(all_users)))
            count += 1

            success, mo_person = await self.set_current_person(uuid=user['uuid'])
            date_list = await self._find_cut_dates(mo_person=mo_person)
            for i in range(0, len(date_list) - 1):
                date = date_list[i]
                mo_engagement = await self._read_engagement(date, mo_person=mo_person)
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

    async def recalculate_primary(self, no_past=False, mo_person=None):
        """
        Re-calculate primary engagement for the entire history of the current user.
        """
        logger.info('Calculate primary engagement: {}'.format(mo_person))
        date_list = await self._find_cut_dates(no_past=no_past, mo_person=mo_person)
        number_of_edits = 0

        for i in range(0, len(date_list) - 1):
            date = date_list[i]
            logger.info('Recalculate primary, date: {}'.format(date))

            mo_engagement = await self._read_engagement(date, mo_person=mo_person)
            # print('Read engagements {}: {}s'.format(i, time.time() - t))

            logger.debug('MO engagement: {}'.format(mo_engagement))
            (min_id, max_rate) = self._calculate_rate_and_ids(mo_engagement, no_past)
            if (min_id is None) or (max_rate is None):
                continue

            fixed = None
            for eng in mo_engagement:
                if no_past and eng['validity']['to']:
                    to = datetime.datetime.strptime(
                        eng['validity']['to'], '%Y-%m-%d')
                    if to < datetime.datetime.now():
                        continue

                if not eng['primary']:
                    # Todo: It would seem this happens for leaves, should we make
                    # a special type for this?
                    eng['primary'] = {'uuid': self.primary_types['non_primary']}

                if eng['primary']['uuid'] == self.primary_types['fixed_primary']:
                    logger.info('Engagment {} is fixed primary'.format(eng['uuid']))
                    fixed = eng['uuid']

            exactly_one_primary = False
            for eng in mo_engagement:
                if no_past and eng['validity']['to']:
                    to = datetime.datetime.strptime(
                        eng['validity']['to'], '%Y-%m-%d')
                    if to < datetime.datetime.now():
                        continue

                if eng['primary']['uuid'] == self.primary_types['no_salary']:
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
                    logger.warning('Engagement type not status0. Will fix.')
                    data = {
                        'primary': {'uuid': self.primary_types['no_salary']},
                        'validity': validity
                    }
                    payload = sd_payloads.engagement(data, eng)
                    logger.debug('Status0 edit payload: {}'.format(payload))
                    response = self.helper._mo_post('details/edit', payload)
                    assert response.status_code == 200
                    logger.info('Status0 fixed')
                    continue

                occupation_rate = 0
                if eng['fraction']:
                    occupation_rate = eng['fraction']

                logger.debug('Current rate and id: {}, {}'.format(occupation_rate,
                                                                  employment_id))

                if occupation_rate == max_rate and employment_id == min_id:
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

                payload = sd_payloads.engagement(data, eng)
                if not payload['data']['primary'] == eng['primary']:
                    logger.debug('Edit payload: {}'.format(payload))
                    response = self.helper._mo_post('details/edit', payload)
                    assert response.status_code in (200, 400)
                    if response.status_code == 400:
                        logger.info('Attempted edit, but no change needed.')
                    number_of_edits += 1
                else:
                    logger.debug('No edit, primary type not changed.')
        return_dict = {mo_person['uuid']: number_of_edits}
        return return_dict

    async def recalculate_all(self, no_past=False):
        """
        Recalculate all primary engagements
        :return: TODO
        """
        all_users = await self.helper.read_all_users()
        all_users = map(itemgetter('uuid'), all_users)

        async def create_task(user_uuid):
            t = time.time()
            success, mo_person = await self.set_current_person(uuid=user_uuid)
            if success:
                status = await self.recalculate_primary(no_past=no_past, mo_person=mo_person)
                logger.debug('Time for primary calculation: {}'.format(time.time() - t))
                return status
            return {}

        tasks = list(map(create_task, all_users))
        statusses = await asyncio.gather(*tasks)
        edit_status = {}
        for status in statusses:
            edit_status.update(status)
        print('Total edits: {}'.format(sum(edit_status.values())))

    async def _cli(self):
        parser = argparse.ArgumentParser(description='Calculate Primary')
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument('--check-all-for-primary',  action='store_true',
                           help='Check all users for a primary engagement')
        group.add_argument('--recalculate-all',  action='store_true',
                           help='Recalculate all users')
        group.add_argument('--recalculate-user', nargs=1, metavar='MO_uuid',
                           help='Recalculate primaries for a user')

        args = vars(parser.parse_args())

        if args.get('recalculate_user'):
            print('Recalculate user')
            t = time.time()
            uuid = args.get('recalculate_user')[0]
            success, mo_person = await self.set_current_person(uuid=uuid)
            if success:
                await self.recalculate_primary(mo_person=mo_person)
                print('Time for primary calculation: {}'.format(time.time() - t))

        if args.get('check_all_for_primary'):
            print('Check all for primary')
            await self.check_all_for_primary()

        if args.get('recalculate_all'):
            print('Check all for primary')
            await self.recalculate_all(no_past=True)


async def main():
    updater = MOPrimaryEngagementUpdater()
    await updater.setup()
    await updater._cli()


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
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
