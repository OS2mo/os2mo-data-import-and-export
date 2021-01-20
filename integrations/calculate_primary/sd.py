from integrations.SD_Lon import sd_common
from integrations.calculate_primary.common import logger, MOPrimaryEngagementUpdater


class SDPrimaryEngagementUpdater(MOPrimaryEngagementUpdater):

    def __init__(self):
        super().__init__()
        self.check_filters = [
            # Filter out no_salary primary, such that only fixed and primary is left
            lambda eng: eng['engagement_type']['uuid'] != primary[3]
        ]

        def remove_past(eng):
            if no_past and eng['validity']['to']:
                to = datetime.datetime.strptime(
                    eng['validity']['to'], '%Y-%m-%d'
                )
                if to < datetime.datetime.now():
                    return False
            return True

        def remove_no_salary(eng):
            if eng['primary']['uuid'] == self.primary_types['no_salary']:
                logger.info('Status 0, no update of primary')
                return False
            return True

        self.calculate_filters = [
            remove_past,
            remove_no_salary,
        ]

    def _find_primary_types(self):
        # Keys are; fixed_primary, primary no_salary non-primary
        primary_types = sd_common.primary_types(self.helper)
        primary = [
            self.primary_types['fixed_primary'],
            self.primary_types['primary'],
            self.primary_types['no_salary']
        ]
        return primary_types, primary

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

    def _handle_non_integer_employment_id(self, validity, eng):
        logger.warning('Engagement type not status0. Will fix.')
        data = {
            'primary': {'uuid': self.primary_types['no_salary']},
            'validity': validity
        }
        payload = edit_engagement(data, eng['uuid'])
        logger.debug('Status0 edit payload: {}'.format(payload))
        response = self.helper._mo_post('details/edit', payload)
        assert response.status_code == 200
        logger.info('Status0 fixed')
        # XXX: If we are counting number of edits, in the main method, why do we
        #      not count this edit here??

    def _is_primary(self, employment_id, eng, min_id, impl_specific):
        max_rate = impl_specific

        occupation_rate = eng.get('fraction', 0)
        logger.debug('Current rate and id: {}, {}'.format(
            occupation_rate, employment_id
        ))

        # XXX: These conditions are not equivalent, and as such we may end
        #      up in a situation where the employee gets no primary at all!
        return occupation_rate == max_rate and employment_id == min_id
