import os
import time
import random
import logging

import ad_templates
import ad_exceptions

from ad_common import AD
from user_names import CreateUserNames
from os2mo_helpers.mora_helpers import MoraHelper


logger = logging.getLogger("AdWriter")

MORA_BASE = os.environ.get('MORA_BASE')
PRIMARY_ENGAGEMENT_TYPE = os.environ.get('PRIMARY_ENGAGEMENT_TYPE')
FORVALTNING_TYPE = os.environ.get('FORVALTNING_TYPE')


if MORA_BASE is None or PRIMARY_ENGAGEMENT_TYPE is None:
    msg = 'Configuration error: MORA_BASE: {}, PRIMARY_ENGAGEMENT_TYPE: {}'
    raise Exception(msg.format(MORA_BASE, PRIMARY_ENGAGEMENT_TYPE))


def _random_password(length=12):
    password = ''
    for _ in range(0, length):
        password += chr(random.randrange(48, 127))
    return password


class ADWriter(AD):
    def __init__(self):
        super().__init__()

        self.helper = MoraHelper(hostname=MORA_BASE, use_cache=False)
        self.name_creator = CreateUserNames(occupied_names=set())
        logger.info('Reading occupied names')
        self.name_creator.populate_occupied_names()
        logger.info('Done reading occupied names')

    def _get_write_setting(self, school=False):
        # TODO: Currently we ignore school
        if not self.all_settings['primary_write']:
            msg = 'Trying to enable write access with broken settings.'
            logger.error(msg)
            raise Exception(msg)
        return self.all_settings['primary_write']

    def _other_attributes(self, mo_values, new_user=False):
        school = False  # TODO
        write_settings = self._get_write_setting(school)
        if new_user:
            other_attributes = ' -OtherAttributes @{'
        else:
            other_attributes = ' -Replace @{'

        other_attributes_fields = [
            (write_settings['forvaltning_field'],
             mo_values['forvaltning'].replace('&', 'og')),
            (write_settings['org_field'], mo_values['location'].replace('&', 'og'))
        ]
        # These two fields are NEVER updated.
        if new_user:
            other_attributes_fields.append(
                (write_settings['uuid_field'], mo_values['uuid'])
            )
            other_attributes_fields.append(
                (write_settings['cpr_field'], mo_values['cpr'])
            )
            
        for field in other_attributes_fields:
            other_attributes += '"{}"="{}";'.format(field[0], field[1])
        other_attributes += '}'
        return other_attributes

    def _wait_for_replication(self, sam):
        t_start = time.time()
        logger.debug('Wait for replication of {}'.format(sam))
        if not self.all_settings['global']['servers']:
            logger.info('No server infomation, falling back to waiting')
            time.sleep(15)
        else:
            # TODO, read from all AD servers and see when user is available
            replication_finished = False
            while not replication_finished:
                for server in self.all_settings['global']['servers']:
                    user = self.get_from_ad(user=sam)
                    logger.debug('Testing {}, found: {}'.format(server, len(user)))
                    if user:
                        logger.debug('Found successfully')
                        replication_finished = True
                    else:
                        logger.debug('Did not find')
                        replication_finished = False
                        time.sleep(0.25)
                        break
        logger.info('replication_finished: {}s'.format(time.time() - t_start))

    def read_ad_informaion_from_mo(self, uuid):
        """
        Retrive the necessary information from MO to contruct a new AD user.
        The final information object should of this type:
        mo_values = {
            'name': ('Martin Lee', 'Gore'),
            'employment_number': '101',
            'uuid': '7ccbd9aa-gd60-4fa1-4571-0e6f41f6ebc0',
            'cpr': '1122334455',
            'title': 'Musiker',
            'location': 'Viborg Kommune\Beskæftigelse, Økonomi & Personale\It-strategisk team\',
            'forvaltning': 'Beskæftigelse, Økonomi & Personale',
            'managerSAM': 'DMILL'
        }
        """
        logger.info('Read information for {}'.format(uuid))
        mo_user = self.helper.read_user(user_uuid=uuid)

        if 'uuid' not in mo_user:
            raise ad_exceptions.UserNotFoundException
        else:
            assert(mo_user['uuid'] == uuid)

        engagements = self.helper.read_user_engagement(uuid)

        found_primary = False
        for engagement in engagements:
            # TODO: Very soon  PRIMARY_ENGAGEMENT_TYPE will be a list
            if engagement['engagement_type']['uuid'] == PRIMARY_ENGAGEMENT_TYPE:
                found_primary = True
                employment_number = engagement['user_key']
                title = engagement['job_function']['name']
                break

        if not found_primary:
            raise ad_exceptions.NoPrimaryEngagementException('User: {}'.format(uuid))

        unit_info = self.helper.read_ou(engagement['org_unit']['uuid'])
        unit = unit_info['name']

        location = ''
        current_unit = unit_info
        forvaltning = None
        while current_unit:
            location = current_unit['name'] + '\\' +location
            if current_unit['org_unit_type']['uuid'] == FORVALTNING_TYPE:
                forvaltning = current_unit['name']
            current_unit = current_unit['parent']
        location = location[:-1]

        manager = self.helper.read_engagement_manager(engagement['uuid'])
        mo_manager_user = self.helper.read_user(user_uuid=manager['uuid'])
        manager_cpr = mo_manager_user['cpr_no']

        # Overrule manager cpr for test!
        # manager_cpr = '1122334459'

        manager_ad_info = self.get_from_ad(cpr=manager_cpr)
        if len(manager_ad_info) == 1:
            manager_sam = manager_ad_info[0]['SamAccountName']
        else:
            print(manager_ad_info)
            raise ad_exceptions.ManagerNotUniqueFromCprException()

        mo_values = {
            'name': (mo_user['givenname'], mo_user['surname']),
            'employment_number': employment_number,
            'uuid': uuid,
            'cpr': mo_user['cpr_no'],
            'title': title,
            'location': location,
            'forvaltning': forvaltning,
            'managerSAM': manager_sam
        }
        return mo_values

    def add_manager_to_user(self, user_sam, manager_sam):
        """
        Mark an existing AD user as manager for an existing AD user.
        :param user_sam: SamAccountName for the employee.
        :param manager_sam: SamAccountName for the manager.
        """
        school = False  # TODO
        bp = self._ps_boiler_plate(school)

        format_rules = {'user_sam': user_sam, 'manager_sam': manager_sam,
                        'ad_server': bp['server']}
        ps_script = self._build_ps(ad_templates.add_manager_template,
                                   school, format_rules)

        response = self._run_ps_script(ps_script)
        return response is {}

    def sync_user(self, mo_uuid):
        """
        Sync MO information into AD
        """
        # TODO: Consider if this is sufficiently similar to create to refactor
        # TODO: SamAccountShould be read from MO!

        school = False  # TODO
        write_settings = self._get_write_setting(school)
        bp = self._ps_boiler_plate(school)

        mo_values = self.read_ad_informaion_from_mo(mo_uuid)

        user_ad_info = self.get_from_ad(cpr=mo_values['cpr'])
        if len(user_ad_info) == 1:
            user_sam = user_ad_info[0]['SamAccountName']
        else:
            msg = 'No SamAccount found for user, unable to sync'
            logger.error(msg)
            raise ad_exceptions.UserNotFoundException(msg)

        edit_user_template = ad_templates.edit_user_template
        replace_attributes = self._other_attributes(mo_values, new_user=False)

        edit_user_string = edit_user_template.format(
            givenname=mo_values['name'][0],
            surname=mo_values['name'][1],
            sam_account_name=user_sam,
            employment_number=mo_values['employment_number']
        )
        edit_user_string = self.remove_redundant(edit_user_string)
        edit_user_string += replace_attributes
        print(edit_user_string)

        ps_script = (
            self._build_user_credential(school) +
            edit_user_string +
            bp['server']
        )
        response = self._run_ps_script(ps_script)
        print(response)
        
        # Works for both create and edit
        self.add_manager_to_user(user_sam=user_sam,
                                 manager_sam=mo_values['managerSAM'])

    def create_user(self, mo_uuid, dry_run=False):
        """
        Create an AD user
        :param mo_uuid: uuid for the MO user we want to add to AD.
        """
        school = False  # TODO
        # TODO: Implement dry_run

        write_settings = self._get_write_setting(school)
        bp = self._ps_boiler_plate(school)
        mo_values = self.read_ad_informaion_from_mo(mo_uuid)

        all_names = mo_values['name'][0].split(' ') + [mo_values['name'][1]]
        sam_account_name = self.name_creator.create_username(all_names,
                                                             dry_run=dry_run)[0]

        existing_sam = self.get_from_ad(user=sam_account_name)
        existing_cpr = self.get_from_ad(cpr=mo_values['cpr'])
        if existing_sam:
            logger.error('SamAccount already in use: {}'.format(sam_account_name))
            ad_exceptions.SamAccountNameNotUnique(sam_account_name)
        if existing_cpr:
            logger.error('cpr already in use: {}'.format(mo_values['cpr']))
            raise ad_exceptions.CprNotNotUnique(mo_values['cpr'])

        create_user_template = ad_templates.create_user_template
        other_attributes = self._other_attributes(mo_values, new_user=True)

        create_user_string = create_user_template.format(
            givenname=mo_values['name'][0],
            surname=mo_values['name'][1],
            sam_account_name=sam_account_name,
            employment_number=mo_values['employment_number']
        )
        create_user_string = self.remove_redundant(create_user_string)
        create_user_string += other_attributes

        ps_script = (
            self._build_user_credential(school) +
            create_user_string +
            bp['server'] +
            bp['path']
        )

        response = self._run_ps_script(ps_script)

        if response == {}:
            self._wait_for_replication(sam_account_name)
            print('Add {} as manager for {}'.format(mo_values['managerSAM'],
                                                    sam_account_name))
            logger.info('Add {} as manager for {}'.format(mo_values['managerSAM'],
                                                          sam_account_name))
            self.add_manager_to_user(user_sam=sam_account_name,
                                     manager_sam=mo_values['managerSAM'])
            return sam_account_name
        else:
            logger.error('Error creating user: {}'.format(response))
            return response

    def set_user_password(self, username, password):
        """
        Set a password for a user.
        :param username: SamAccountName for the user.
        :param password: The password to assign to the user.
        :return: True if success, otherwise False
        """
        school = False  # TODO

        format_rules = {'username': username, 'password': password}
        ps_script = self._build_ps(ad_templates.set_password_template,
                                   school, format_rules)
        response = self._run_ps_script(ps_script)
        if not response:
            return True
        else:
            logger.error('Failed to set password!: {}'.format(response))
            return False

    def enable_user(self, username):
        """
        Disable an AD account.
        :param username: SamAccountName of the account to be disabled
        """
        school = False  # TODO

        format_rules = {'username': username}
        ps_script = self._build_ps(ad_templates.enable_user_template,
                                   school, format_rules)
        response = self._run_ps_script(ps_script)
        if not response:
            return True
        else:
            logger.error('Failed to set enable account!: {}'.format(response))
            return False

    def delete_user(self, username):
        """
        Delete an AD account. Only to be used for service purpose, actual
        AD integration should never delete a user, but only mark it for
        deletetion.
        :param username: SamAccountName of the account to be deleted
        """
        format_rules = {'username': username}
        ps_script = self._build_ps(ad_templates.delete_user_template,
                                   school=False, format_rules=format_rules)
        response = self._run_ps_script(ps_script)
        # TODO: Should we make a read to confirm the user i gone?
        if not response:
            return True
        else:
            logger.error('Failed to delete account!: {}'.format(response))
            return False


if __name__ == '__main__':
    ad_writer = ADWriter()
    
    # ad_writer.add_manager_to_user('CBAKT', 'OBRAP')

    # ad_writer.create_user(uuid)

    # TODO: Test this by sync'ing other users into existing accounts.
    # ad_writer.sync_user(uuid)

    # print(ad_writer.get_from_ad(user='MLEEG')[0]['Enabled'])

    # ad_writer.set_user_password('MSLEG', _random_password())
    # ad_writer.enable_user('OBRAP')

    ad_writer.delete_user('LSKÅJ')
