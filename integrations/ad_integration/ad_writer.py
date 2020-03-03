import re
import json
import time
import random
import logging
import pathlib
import datetime
import argparse

import ad_logger
import ad_templates
import ad_exceptions

from ad_common import AD
from user_names import CreateUserNames
from os2mo_helpers.mora_helpers import MoraHelper

logger = logging.getLogger("AdWriter")


def _random_password(length=12):
    password = ''
    for _ in range(0, length):
        password += chr(random.randrange(48, 127))
    return password


class ADWriter(AD):
    def __init__(self):
        super().__init__()

        cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
        if not cfg_file.is_file():
            raise Exception('No setting file')
        self.settings = json.loads(cfg_file.read_text())
        # self.pet = self.settings['integrations.ad.write.primary_types']

        self.helper = MoraHelper(hostname=self.settings['mora.base'],
                                 use_cache=False)
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

    def _other_attributes(self, mo_values, user_sam, new_user=False):
        school = False  # TODO
        write_settings = self._get_write_setting(school)
        if new_user:
            other_attributes = ' -OtherAttributes @{'
        else:
            other_attributes = ' -Replace @{'

        # other_attributes_fields = [
        #     (write_settings['forvaltning_field'],
        #      mo_values['forvaltning'].replace('&', 'og')),
        #     (write_settings['org_field'], mo_values['location'].replace('&', 'og'))
        # ]
        other_attributes_fields = [
            (write_settings['forvaltning_field'],
             mo_values['forvaltning']),
            (write_settings['org_field'], mo_values['location'])
        ]

        # Add SAM to mo_values
        mo_values['name_sam'] = '{} - {}'.format(mo_values['full_name'], user_sam)

        # Local fields for MO->AD sync'ing
        named_sync_fields = self.settings.get(
            'integrations.ad_writer.mo_to_ad_fields', {})

        for mo_field, ad_field in named_sync_fields.items():
            other_attributes_fields.append(
                (ad_field, mo_values[mo_field])
            )

        # These fields are NEVER updated.
        if new_user:
            # This needs extended permissions, do we need it?
            # other_attributes_fields.append(('pwdLastSet', '0'))
            other_attributes_fields.append(
                ('UserPrincipalName',
                 '{}@{}'.format(user_sam, write_settings['upn_end']))
            )
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
                if time.time() - t_start > 60:
                    logger.error('Replication error')
                    raise ad_exceptions.ReplicationFailedException()

                for server in self.all_settings['global']['servers']:
                    user = self.get_from_ad(user=sam, server=server)
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

    def read_ad_information_from_mo(self, uuid, read_manager=True):
        """
        Retrive the necessary information from MO to contruct a new AD user.
        The final information object should of this type, notice that end-date
        is not necessarily for the current primary engagement, but the end-date
        of the longest running currently known primary engagement:
        mo_values = {
            'name': ('Martin Lee', 'Gore'),
            'employment_number': '101',
            'uuid': '7ccbd9aa-gd60-4fa1-4571-0e6f41f6ebc0',
            'end_date': 2089-11-11,
            'cpr': '1122334455',
            'title': 'Musiker',
            'location': 'Viborg Kommune\Forvalting\Enhed\',
            'forvaltning': 'Beskæftigelse, Økonomi & Personale',
            'manager_sam': 'DMILL'
        }
        """
        logger.info('Read information for {}'.format(uuid))
        mo_user = self.helper.read_user(user_uuid=uuid)

        if 'uuid' not in mo_user:
            raise ad_exceptions.UserNotFoundException
        else:
            assert(mo_user['uuid'] == uuid)

        engagements = self.helper.read_user_engagement(uuid, calculate_primary=True)

        found_primary = False
        for engagement in engagements:
            # engagement_type = engagement['engagement_type']['uuid']
            if engagement['is_primary']:
                found_primary = True
                employment_number = engagement['user_key']
                title = engagement['job_function']['name']
                end_date = engagement['validity']['to']
                if end_date is None:
                    end_date = '9999-12-31'

        if not found_primary:
            raise ad_exceptions.NoPrimaryEngagementException('User: {}'.format(uuid))

        # Now, calculate final end date for any primary engagement
        future_engagements = self.helper.read_user_engagement(uuid, read_all=True,
                                                              skip_past=True)
        for eng in future_engagements:
            if engagement['is_primary']:
                current_end = eng['validity']['to']
                if current_end is None:
                    current_end = '9999-12-31'

                if (
                        datetime.datetime.strptime(current_end, '%Y-%m-%d') >
                        datetime.datetime.strptime(end_date, '%Y-%m-%d')
                ):
                    end_date = current_end

        unit_info = self.helper.read_ou(engagement['org_unit']['uuid'])
        unit_name = unit_info['name']
        unit_uuid = unit_info['uuid']
        unit_user_key = unit_info['user_key']

        # misc = self.helper.read_ou_address(unit_uuid, scope='TEXT',
        # return_all=True)
        # www = self.helper.read_ou_address(unit_uuid, scope='WWW', return_all=True)
        # phone = self.helper.read_ou_address(unit_uuid, scope='PHONE',
        # return_all=True)
        email = self.helper.read_ou_address(unit_uuid, scope='EMAIL',
                                            return_all=True)
        postal = self.helper.read_ou_address(unit_uuid, scope='DAR',
                                             return_all=False)

        unit_secure_email = None
        unit_public_email = None
        for mail in email:
            if mail['visibibility'] is None:
                # If visibility is not set, we assume it is non-public.
                unit_secure_email = mail['value']
            else:
                if mail['visibibility']['scope'] == 'PUBLIC':
                    unit_public_email = mail['value']
                if mail['visibibility']['scope'] == 'SECRET':
                    unit_secure_email = mail['value']

        postal_code = city = streetname = 'Ukendt'
        if postal:
            try:
                postal_code = re.findall('[0-9]{4}', postal['Adresse'])[0]
                city_pos = postal['Adresse'].find(postal_code) + 5
                city = postal['Adresse'][city_pos:]
                streetname = postal['Adresse'][:city_pos - 7]
            except IndexError:

                logger.error('Unable to read adresse from MO (no access to DAR?)')

        location = ''
        current_unit = unit_info
        forvaltning = 'Ingen'
        while current_unit:
            location = current_unit['name'] + '\\' + location
            current_type = current_unit['org_unit_type']
            current_level = current_unit['org_unit_level']
            if current_level is None:
                current_level =  {'uuid': None}
            if self.settings['integrations.ad.write.forvaltning_type'] in (
                    current_type['uuid'],
                    current_level['uuid']
            ):
                forvaltning = current_unit['name']
            current_unit = current_unit['parent']
        location = location[:-1]

        manager_name = None
        manager_sam = None
        manager_mail = None
        if read_manager:
            try:
                manager = self.helper.read_engagement_manager(engagement['uuid'])
            except KeyError:
                logger.info('No managers found')
                read_manager = False

        if read_manager:
            manager_name = manager['Navn']
            mo_manager_user = self.helper.read_user(user_uuid=manager['uuid'])
            manager_cpr = mo_manager_user['cpr_no']
            manager_mail_dict = self.helper.get_e_address(manager['uuid'],
                                                          scope='EMAIL')
            if manager_mail_dict:
                manager_mail = manager_mail_dict['value']

            manager_ad_info = self.get_from_ad(cpr=manager_cpr)
            if len(manager_ad_info) == 1:
                manager_sam = manager_ad_info[0]['SamAccountName']
            else:
                msg = 'Searching for {}, found in AD: {}'
                logger.debug(msg.format(manager['Navn'], manager_ad_info))
                raise ad_exceptions.ManagerNotUniqueFromCprException()

        mo_values = {
            'name': (mo_user['givenname'], mo_user['surname']),
            'full_name': '{} {}'.format(mo_user['givenname'], mo_user['surname']),
            'employment_number': employment_number,
            'end_date': end_date,
            'uuid': uuid,
            'cpr': mo_user['cpr_no'],
            'title': title,
            'unit': unit_name,
            'unit_uuid': unit_uuid,
            'unit_user_key': unit_user_key,
            'unit_public_email': unit_public_email,
            'unit_secure_email': unit_secure_email,
            'unit_postal_code': postal_code,
            'unit_city': city,
            'unit_streetname': streetname,
            # UNIT PHONE NUMBER
            # UNIT WEB PAGE
            'location': location,
            'forvaltning': forvaltning,
            'manager_name': manager_name,
            'manager_sam': manager_sam,
            'manager_mail': manager_mail
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

    def sync_user(self, mo_uuid, user_ad_info=None, sync_manager=True):
        """
        Sync MO information into AD
        """
        school = False  # TODO
        bp = self._ps_boiler_plate(school)

        mo_values = self.read_ad_information_from_mo(mo_uuid)

        if user_ad_info is None:
            logger.debug('No AD information supplied, will look it up')
            user_sam = self._find_unique_user(mo_values['cpr'])
        else:
            user_sam = user_ad_info['SamAccountName']

        edit_user_template = ad_templates.edit_user_template
        replace_attributes = self._other_attributes(mo_values, user_sam,
                                                    new_user=False)

        edit_user_string = edit_user_template.format(
            givenname=mo_values['name'][0],
            surname=mo_values['name'][1],
            sam_account_name=user_sam,
            employment_number=mo_values['employment_number']
        )
        edit_user_string = self.remove_redundant(edit_user_string)
        edit_user_string += replace_attributes

        ps_script = (
            self._build_user_credential(school) +
            edit_user_string +
            bp['server']
        )
        logger.debug('Sync user, ps_script: {}'.format(ps_script))

        response = self._run_ps_script(ps_script)
        logger.debug('Response from sync: {}'.format(response))

        if sync_manager:
            self.add_manager_to_user(user_sam=user_sam,
                                     manager_sam=mo_values['manager_sam'])
        return (True, 'Sync completed')

    def create_user(self, mo_uuid, create_manager, dry_run=False):
        """
        Create an AD user
        :param mo_uuid: uuid for the MO user we want to add to AD.
        :param create_manager: If True, an AD link will be added between the user
        object and the AD object of the users manager.
        :param dry_run: Not yet implemented. Should return whether the user is
        expected to be able to be created in AD and the expected SamAccountName.
        :return: The generated SamAccountName for the new user
        """
        school = False  # TODO
        # TODO: Implement dry_run

        bp = self._ps_boiler_plate(school)
        mo_values = self.read_ad_information_from_mo(mo_uuid, create_manager)

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
        other_attributes = self._other_attributes(mo_values, sam_account_name,
                                                  new_user=True)

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
        if not response == {}:
            msg = 'Create user failed, message: {}'.format(response)
            logger.error(msg)
            return (False, msg)

        if create_manager:
            self._wait_for_replication(sam_account_name)
            print('Add {} as manager for {}'.format(mo_values['manager_sam'],
                                                    sam_account_name))
            logger.info('Add {} as manager for {}'.format(mo_values['manager_sam'],
                                                          sam_account_name))
            self.add_manager_to_user(user_sam=sam_account_name,
                                     manager_sam=mo_values['manager_sam'])

        return (True, sam_account_name)

    def add_ad_to_user_it_systems(self, username):
        # TODO: We need a function to write the SamAccount to the user's
        # IT-systems. This is most likely most elegantly done by importing
        # the AD->MO sync tool
        pass

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
            return (True, 'Password updated')
        else:
            msg = 'Failed to set password!: {}'.format(response)
            logger.error(msg)
            return (False, msg)

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
            return (True, 'Account enabled')
        else:
            msg = 'Failed to set enable account!: {}'.format(response)
            logger.error(msg)
            return (False, msg)

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
        # TODO: Should we make a read to confirm the user is gone?
        if not response:
            return (True, 'User deleted')
        else:
            logger.error('Failed to delete account!: {}'.format(response))
            return (False, 'Failed to delete')

    def _cli(self):
        """
        Command line interface for the AD writer class.
        """
        parser = argparse.ArgumentParser(description='AD Writer')
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument('--create-user-with-manager', nargs=1, metavar='MO_uuid',
                           help='Create a new user in AD, also assign a manager')
        group.add_argument('--create-user', nargs=1, metavar='MO_uuid',
                           help='Create a new user in AD, do not assign a manager')
        group.add_argument('--sync-user', nargs=1, metavar='MO uuid',
                           help='Sync relevant fields from MO to AD')
        group.add_argument('--delete-user', nargs=1, metavar='User_SAM')
        group.add_argument('--read-ad-information', nargs=1, metavar='User_SAM')
        group.add_argument('--add-manager-to-user',  nargs=2,
                           metavar=('Manager_SAM', 'User_SAM'))

        args = vars(parser.parse_args())

        if args.get('create_user_with_manager'):
            print('Create_user_with_manager:')
            uuid = args.get('create_user_with_manager')[0]
            status = self.create_user(uuid, create_manager=True)
            # TODO: execute custom script? Or should this be done in
            # two steps.
            print(status[1])

        if args.get('create_user'):
            print('Create user, no link to manager:')
            uuid = args.get('create_user')[0]
            status = self.create_user(uuid, create_manager=False)
            print(status[1])

        if args.get('sync_user'):
            print('Sync MO fields to AD')
            uuid = args.get('sync_user')[0]
            status = self.sync_user(uuid)
            print(status[1])

        if args.get('delete_user'):
            print('Deleting user:')
            sam = args.get('delete_user')[0]
            status = self.delete_user(sam)
            print(status[1])

        if args.get('read_ad_information'):
            print('AD information on user:')
            sam = args.get('read_ad_information')[0]
            user = self.get_from_ad(user=sam)
            if not user:
                print('User not found')
            else:
                for key, value in sorted(user[0].items()):
                    print('{}: {}'.format(key, value))

        if args.get('add_manager_to_user'):
            manager = args['add_manager_to_user'][0]
            user = args['add_manager_to_user'][1]
            print('{} is now set as manager for {}'.format(manager, user))
            self.add_manager_to_user(manager_sam=manager, user_sam=user)

        # TODO: Enable a user, including setting a random password
        # ad_writer.set_user_password('MSLEG', _random_password())
        # ad_writer.enable_user('OBRAP')


if __name__ == '__main__':
    ad_logger.start_logging('ad_writer.log')

    ad_writer = ADWriter()
    ad_writer._cli()
