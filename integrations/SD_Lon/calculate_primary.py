import click
import time
import json
import pathlib
import logging
import datetime

from tqdm import tqdm
from more_itertools import ilen, pairwise

from integrations.SD_Lon import sd_common
from integrations.SD_Lon import sd_payloads

from os2mo_helpers.mora_helpers import MoraHelper


logger = logging.getLogger("updatePrimaryEngagements")
LOG_LEVEL = logging.DEBUG
LOG_FILE = 'calculate_primary.log'


def setup_logging():
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


class MOPrimaryEngagementUpdater(object):
    def __init__(self):
        settings = sd_common.load_settings()
        mora_base = settings['mora.base']

        self.helper = MoraHelper(hostname=mora_base, use_cache=False)
        self.org_uuid = self.helper.read_organisation()

        self.mo_person = None

        # Keys are; fixed_primary, primary no_salary non-primary
        self.primary_types = sd_common.primary_types(self.helper)
        self.primary = [
            self.primary_types['fixed_primary'],
            self.primary_types['primary'],
            self.primary_types['no_salary']
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
        # print('Read user: {}s'.format(time.time() - t))
        self.mo_person = mo_person
        success = mo_person is not None
        return success

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
        for user in tqdm(all_users):
            self.set_current_person(uuid=user['uuid'])

            # List of cut dates, excluding the very last one
            date_list = self.helper.find_cut_dates(uuid=user['uuid'])
            date_list = date_list[:-1]
            # Map all our dates, to their corresponding engagements.
            mo_engagements = list(map(self._read_engagement, date_list))
            # Only keep engagements, which are primary
            primary_mo_engagements = filter(
                lambda eng: eng['engagement_type']['uuid'] in self.primary,
                mo_engagements
            )
            # Count number of primary engagements in the iterator
            primary_count = ilen(primary_mo_engagements)

            if primary_count == 0:
                print('No primary for {} at {}'.format(user['uuid'], date))
            elif primary_count > 1:
                # This will typically happen because of both a primary and a status0
                logger.info('{} has more than one primary'.format(user['uuid']))
                extra_primary_mo_engagements = filter(
                    lambda eng: eng['engagement_type']['uuid'] in self.primary[0:2],
                    mo_engagements
                )
                extra_primary_count = ilen(extra_primary_mo_engagements)
                if extra_primary_count == 1:
                    logger.info('Only one primary was different from status 0')
                elif extra_primary_count > 1:
                    print('Too many primaries for {} at {}'.format(
                        user['uuid'], date
                    ))

    def recalculate_primary(self, no_past=False):
        """
        Re-calculate primary engagement for the entire history of the current user.
        """
        def remove_past(eng):
            if no_past and eng['validity']['to']:
                to = datetime.datetime.strptime(
                    eng['validity']['to'], '%Y-%m-%d'
                )
                if to < datetime.datetime.now():
                    return False
            return True

        logger.info('Calculate primary engagement: {}'.format(self.mo_person))
        uuid = self.mo_person['uuid']
        date_list = self.helper.find_cut_dates(uuid, no_past=no_past)
        number_of_edits = 0

        for date, next_date in pairwise(date_list):
            logger.info('Recalculate primary, date: {}'.format(date))

            mo_engagements = self._read_engagement(date)
            # print('Read engagements {}: {}s'.format(i, time.time() - t))

            logger.debug('MO engagement: {}'.format(mo_engagements))
            (min_id, max_rate) = self._calculate_rate_and_ids(mo_engagements, no_past)
            if (min_id is None) or (max_rate is None):
                continue

            mo_engagements = list(filter(remove_past, mo_engagements))

            # Enrich engagements with primary, if required
            # TODO: It would seem this happens for leaves, should we make a
            #       special type for this?
            # XXX: This should probably not be done as a side-effect!
            for eng in mo_engagements:
                if not eng['primary']:
                    eng['primary'] = {'uuid': self.primary_types['non_primary']}

            # XXX: Should we detect and handle multiple fixed primary engagements,
            # or just pick the last one here, and why the last one??
            fixed = None
            for eng in mo_engagements:
                if eng['primary']['uuid'] == self.primary_types['fixed_primary']:
                    logger.info('Engagement {} is fixed primary'.format(eng['uuid']))
                    fixed = eng['uuid']

            def remove_no_salary(eng):
                if eng['primary']['uuid'] == self.primary_types['no_salary']:
                    logger.info('Status 0, no update of primary')
                    return False
                return True

            exactly_one_primary = False
            mo_engagements = filter(remove_no_salary, mo_engagements)
            for eng in mo_engagements:
                to = datetime.datetime.strftime(
                    next_date - datetime.timedelta(days=1), "%Y-%m-%d"
                )
                if next_date == datetime.datetime(9999, 12, 30, 0, 0):
                    to = None
                validity = {
                    'from': datetime.datetime.strftime(date, "%Y-%m-%d"),
                    'to': to
                }

                if 'user_key' not in eng:
                    break  # Why break instead of continue?!

                try:
                    # non-integer user keys should universally be status0
                    # XXX: So why are they not? - Is this invariant being broken??
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
                    # XXX: If we are counting number of edits, why do we not count 
                    #      this edit here??
                    continue

                occupation_rate = eng.get('fraction', 0)
                logger.debug('Current rate and id: {}, {}'.format(
                    occupation_rate, employment_id
                ))

                # XXX: These conditions are not equivalent, and as such we may end
                #      up in a situation where the employee gets no primary at all!
                if occupation_rate == max_rate and employment_id == min_id:
                    assert(exactly_one_primary is False)
                    logger.debug('Primary is: {}'.format(employment_id))
                    exactly_one_primary = True
                    current_type = self.primary_types['primary']
                else:
                    logger.debug('{} is not primary'.format(employment_id))
                    current_type = self.primary_types['non_primary']

                if fixed is not None and eng['uuid'] != fixed:
                    # A fixed primary exists, but this is not it.
                    # XXX: Really it could be if multiple fixed exists, it just does
                    #      not happen to be 'the last one' for some ordering.
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


@click.command()
@click.option("--check-all", is_flag=True, type=click.BOOL, help="Check all users")
@click.option("--recalculate-all", is_flag=True, type=click.BOOL, help="Recalculate all users")
@click.option("--recalculate-user", type=click.UUID, help="Recalculate one user")
def calculate_primary(check_all, recalculate_all, recalculate_user):
    """Tool to work with primary engagement(s)."""
    setup_logging()

    num_set = sum(map(bool, [check_all, recalculate_all, recalculate_user]))
    if num_set == 0:
        raise click.ClickException("Please provide atleast one argument")
    if num_set > 1:
        raise click.ClickException("Flags are mutually exclusive")

    updater = MOPrimaryEngagementUpdater()
    if check_all:
        print('Check all for primary')
        updater.check_all_for_primary()

    if recalculate_all:
        print('Recalculate all')
        updater.recalculate_all(no_past=True)

    if recalculate_user:
        print('Recalculate user')
        t = time.time()
        updater.set_current_person(uuid=recalculate_user)
        updater.recalculate_primary()
        print('Time for primary calculation: {}'.format(time.time() - t))


if __name__ == '__main__':
    calculate_primary()
