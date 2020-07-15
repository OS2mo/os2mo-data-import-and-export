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

from ad_template_engine import template_create_user

from integrations.ad_integration.ad_exceptions import CprNotNotUnique
from integrations.ad_integration.ad_exceptions import UserNotFoundException
from integrations.ad_integration.ad_exceptions import CprNotFoundInADException
from integrations.ad_integration.ad_exceptions import ReplicationFailedException
from integrations.ad_integration.ad_exceptions import NoPrimaryEngagementException
from integrations.ad_integration.ad_exceptions import SamAccountNameNotUnique
from integrations.ad_integration.ad_exceptions import (
    ManagerNotUniqueFromCprException)

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
    def __init__(self, lc=None, lc_historic=None, **kwargs):
        super().__init__(**kwargs)
        self.opts = dict(**kwargs)

        self.settings = self.all_settings
        # self.pet = self.settings['integrations.ad.write.primary_types']

        self.lc = lc
        self.lc_historic = lc_historic

        self.helper = MoraHelper(hostname=self.settings['mora.base'],
                                 use_cache=False)
        self._init_name_creator()

    def _init_name_creator(self):
        self.name_creator = CreateUserNames(occupied_names=set())
        logger.info('Reading occupied names')
        self.name_creator.populate_occupied_names(**self.opts)
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
        #     (write_settings['level2orgunit_field'],
        #      mo_values['level2orgunit'].replace('&', 'og')),
        #     (write_settings['org_field'], mo_values['location'].replace('&', 'og'))
        # ]
        other_attributes_fields = [
            (write_settings['level2orgunit_field'],
             mo_values['level2orgunit']),
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
            # If local settings dictates a separator, we add it directly to the
            # power-shell code.
            ad_cpr = '{}{}{}'.format(
                mo_values['cpr'][0:6],
                self.settings['integrations.ad.cpr_separator'],
                mo_values['cpr'][6:10]
            )
            other_attributes_fields.append(
                (write_settings['cpr_field'], ad_cpr)
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
                    raise ReplicationFailedException()

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

    def _read_user(self, uuid):
        if self.lc:
            if uuid not in self.lc.users:
                raise UserNotFoundException()

            lc_user = self.lc.users[uuid]
            mo_user = {
                'uuid': uuid,
                'name': lc_user['navn'],
                'surname': lc_user['efternavn'],
                'givenname': lc_user['fornavn'],
                'cpr_no': lc_user['cpr']
            }
        else:
            mo_user = self.helper.read_user(user_uuid=uuid)
            if 'uuid' not in mo_user:
                raise UserNotFoundException()
            else:
                assert(mo_user['uuid'] == uuid)
        return mo_user

    def _find_ad_user(self, cpr, ad_dump):
        ad_info = []
        if ad_dump is not None:
            for user in ad_dump:
                if user.get(self.settings['integrations.ad.cpr_field']) == cpr:
                    ad_info.append(user)
        else:
            ad_info = self.get_from_ad(cpr=cpr)

        if not ad_info:
            msg = 'Found no account for {}'.format(cpr)
            logger.error(msg)
            raise CprNotFoundInADException()
        if len(ad_info) > 1:
            raise CprNotNotUnique
        return ad_info

    def _find_unit_info(self, eng_org_unit):
        level2orgunit = 'Ingen'
        unit_info = {}
        if self.lc:
            unit_name = self.lc.units[eng_org_unit][0]['name']
            unit_user_key = self.lc.units[eng_org_unit][0]['user_key']
            location = self.lc.units[eng_org_unit][0]['location']

            # We initialize parent as the unit itself to ensure to catch if
            # a person is engaged directly in a level2org
            parent_uuid = self.lc.units[eng_org_unit][0]['uuid']
            while parent_uuid is not None:
                parent_unit = self.lc.units[parent_uuid][0]
                if self.settings['integrations.ad.write.level2orgunit_type'] in (
                        parent_unit['unit_type'],
                        parent_unit['level']
                ):
                    level2orgunit = parent_unit['name']
                parent_uuid = parent_unit['parent']
        else:
            mo_unit_info = self.helper.read_ou(eng_org_unit)
            unit_name = mo_unit_info['name']
            unit_user_key = mo_unit_info['user_key']

            location = ''
            current_unit = mo_unit_info
            while current_unit:
                location = current_unit['name'] + '\\' + location
                current_type = current_unit['org_unit_type']
                current_level = current_unit['org_unit_level']
                if current_level is None:
                    current_level = {'uuid': None}
                if self.settings['integrations.ad.write.level2orgunit_type'] in (
                        current_type['uuid'],
                        current_level['uuid']
                ):
                    level2orgunit = current_unit['name']
                current_unit = current_unit['parent']
            location = location[:-1]

        unit_info = {
            'name': unit_name,
            'user_key': unit_user_key,
            'location': location,
            'level2orgunit': level2orgunit
        }
        return unit_info

    def _read_user_addresses(self, eng_org_unit):
        addresses = {}
        if self.lc:
            email = []
            postal = {}
            for addr in self.lc.addresses.values():
                if addr[0]['unit'] == eng_org_unit:
                    if addr[0]['scope'] == 'DAR':
                        postal = {'Adresse': addr[0]['value']}
                    if addr[0]['scope'] == 'E-mail':
                        visibility = addr[0]['visibility']
                        visibility_class = None
                        if visibility is not None:
                            visibility_class = self.lc.classes[visibility]
                        email.append(
                            {
                                'visibility': visibility_class,
                                'value': addr[0]['value']
                            }
                        )
        else:
            email = self.helper.read_ou_address(eng_org_unit, scope='EMAIL',
                                                return_all=True)
            postal = self.helper.read_ou_address(eng_org_unit, scope='DAR',
                                                 return_all=False)

        unit_secure_email = None
        unit_public_email = None
        for mail in email:
            if mail['visibility'] is None:
                # If visibility is not set, we assume it is non-public.
                unit_secure_email = mail['value']
            else:
                if mail['visibility']['scope'] == 'PUBLIC':
                    unit_public_email = mail['value']
                if mail['visibility']['scope'] == 'SECRET':
                    unit_secure_email = mail['value']

        addresses = {
            'unit_secure_email': unit_secure_email,
            'unit_public_email': unit_public_email,
            'postal': postal
        }
        return addresses

    def _find_end_date(self, uuid):
        end_date = '1800-01-01'
        # Now, calculate final end date for any primary engagement
        if self.lc_historic is not None:
            all_engagements = []
            for eng in self.lc_historic.engagements.values():
                if eng[0]['user'] == uuid:  # All elements have the same user
                    all_engagements += eng
            for eng in all_engagements:
                current_end = eng['to_date']
                if current_end is None:
                    current_end = '9999-12-31'

                if (
                        datetime.datetime.strptime(current_end, '%Y-%m-%d') >
                        datetime.datetime.strptime(end_date, '%Y-%m-%d')
                ):
                    end_date = current_end

        else:
            future_engagements = self.helper.read_user_engagement(uuid,
                                                                  read_all=True,
                                                                  skip_past=True)
            for eng in future_engagements:
                current_end = eng['validity']['to']
                if current_end is None:
                    current_end = '9999-12-31'

                if (
                        datetime.datetime.strptime(current_end, '%Y-%m-%d') >
                        datetime.datetime.strptime(end_date, '%Y-%m-%d')
                ):
                    end_date = current_end
        return end_date

    def read_ad_information_from_mo(self, uuid, read_manager=True, ad_dump=None):
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
            'level2orgunit: 'Beskæftigelse, Økonomi & Personale',
            'manager_sam': 'DMILL'
        }
        """
        logger.info('Read information for {}'.format(uuid))
        mo_user = self._read_user(uuid)

        force_mo = False
        no_active_engagements = True
        if self.lc:
            for eng in self.lc.engagements.values():
                if eng[0]['user'] == uuid:
                    no_active_engagements = False
                    if eng[0]['primary_boolean']:
                        found_primary = True
                        employment_number = eng[0]['user_key']
                        title = self.lc.classes[eng[0]['job_function']]['title']
                        eng_org_unit = eng[0]['unit']
                        eng_uuid = eng[0]['uuid']
            if no_active_engagements:
                for eng in self.lc_historic.engagements.values():
                    if eng[0]['user'] == uuid:
                        logger.info('Found future engagement')
                        force_mo = True

        if force_mo or not self.lc:
            engagements = self.helper.read_user_engagement(
                uuid, calculate_primary=True, read_all=True, skip_past=True)
            found_primary = False
            for engagement in engagements:
                no_active_engagements = False
                if engagement['is_primary']:
                    found_primary = True
                    employment_number = engagement['user_key']
                    title = engagement['job_function']['name']
                    eng_org_unit = engagement['org_unit']['uuid']
                    eng_uuid = engagement['uuid']

        if no_active_engagements:
            logger.info('No active engagements found')
            return None

        if not found_primary:
            raise NoPrimaryEngagementException('User: {}'.format(uuid))

        end_date = self._find_end_date(uuid)

        unit_info = self._find_unit_info(eng_org_unit)
        addresses = self._read_user_addresses(eng_org_unit)

        postal_code = city = streetname = 'Ukendt'
        if addresses.get('postal'):
            postal = addresses['postal']
            try:
                postal_code = re.findall('[0-9]{4}', postal['Adresse'])[0]
                city_pos = postal['Adresse'].find(postal_code) + 5
                city = postal['Adresse'][city_pos:]
                streetname = postal['Adresse'][:city_pos - 7]
            except IndexError:
                logger.error('Unable to read adresse from MO (no access to DAR?)')
            except TypeError:
                logger.error('Unable to read adresse from MO (no access to DAR?)')

        manager_info = {
            'name': None,
            'sam':  None,
            'mail': None,
            'cpr': None
        }
        if read_manager:
            if self.lc:
                try:
                    manager_uuid = self.lc.managers[
                        self.lc.units[eng_org_unit][0]['acting_manager_uuid']
                    ][0]['user']

                    parent_uuid = self.lc.units[eng_org_unit][0]['parent']
                    while manager_uuid == mo_user['uuid']:
                        if parent_uuid is None:
                            logger.info('This person has no manager!')
                            read_manager = False
                            break

                        msg = 'Self manager, keep searching: {}!'
                        logger.info(msg.format(mo_user))
                        parent_unit = self.lc.units[parent_uuid][0]
                        manager_uuid = self.lc.managers[
                            parent_unit['acting_manager_uuid']][0]['user']

                        parent_uuid = self.lc.units[parent_uuid][0]['parent']
                except KeyError:
                    # TODO: Report back that manager was not found!
                    logger.info('No managers found')
                    read_manager = False
            else:
                try:
                    manager = self.helper.read_engagement_manager(eng_uuid)
                    manager_uuid = manager['uuid']
                except KeyError:
                    logger.info('No managers found')
                    read_manager = False

        if read_manager:
            mo_manager_user = self._read_user(manager_uuid)
            manager_info['name'] = mo_manager_user['name']
            manager_info['cpr'] = mo_manager_user['cpr_no']

            if self.lc:
                manager_mail_dict = {}
                for addr in self.lc.addresses.values():
                    if addr[0]['user'] == manager_uuid:
                        manager_mail_dict = addr[0]
            else:
                manager_mail_dict = self.helper.get_e_address(manager_uuid,
                                                              scope='EMAIL')
            if manager_mail_dict:
                manager_info['mail'] = manager_mail_dict['value']

            try:
                manager_ad_info = self._find_ad_user(cpr=manager_info['cpr'],
                                                     ad_dump=ad_dump)
            except CprNotFoundInADException:
                manager_ad_info = []

            if len(manager_ad_info) == 1:
                manager_info['sam'] = manager_ad_info[0]['SamAccountName']
            else:
                msg = 'Searching for {}, found in AD: {}'
                logger.debug(msg.format(manager_info['name'], manager_ad_info))
                raise ManagerNotUniqueFromCprException()

        mo_values = {
            'read_manager': read_manager,
            'name': (mo_user['givenname'], mo_user['surname']),
            'full_name': '{} {}'.format(mo_user['givenname'], mo_user['surname']),
            'employment_number': employment_number,
            'end_date': end_date,
            'uuid': uuid,
            'cpr': mo_user['cpr_no'],
            'title': title,
            'unit': unit_info['name'],
            'unit_uuid': eng_org_unit,
            'unit_user_key': unit_info['user_key'],
            'unit_public_email': addresses['unit_public_email'],
            'unit_secure_email': addresses['unit_secure_email'],
            'unit_postal_code': postal_code,
            'unit_city': city,
            'unit_streetname': streetname,
            # UNIT PHONE NUMBER
            # UNIT WEB PAGE
            'location': unit_info['location'],
            'level2orgunit': unit_info['level2orgunit'],
            'manager_sam': manager_info['sam'],
            'manager_cpr': manager_info['cpr'],
            'manager_name': manager_info['name'],
            'manager_mail': manager_info['mail']
        }
        return mo_values

    def add_manager_to_user(self, user_sam, manager_sam):
        """
        Mark an existing AD user as manager for an existing AD user.
        :param user_sam: SamAccountName for the employee.
        :param manager_sam: SamAccountName for the manager.
        """
        school = False  # TODO
        format_rules = {'user_sam': user_sam, 'manager_sam': manager_sam}
        ps_script = self._build_ps(ad_templates.add_manager_template,
                                   school, format_rules)

        response = self._run_ps_script(ps_script)
        return response is {}

    def _cf(self, ad_field, value, ad):
        logger.info('Check AD field: {}'.format(ad_field))
        mismatch = {}
        if value is None:
            msg = 'Value for {} is None-type replace to string None'
            logger.debug(msg.format(ad_field))
            value = 'None'
        if not ad.get(ad_field) == value:
            msg = '{}: AD value: {}, does not match MO value: {}'
            logger.info(msg.format(ad_field, ad.get(ad_field), value))
            mismatch = {
                ad_field: (
                    ad.get(ad_field),
                    value
                )
            }
        return mismatch

    def _sync_compare(self, mo_values, ad_dump):
        school = False  # TODO
        write_settings = self._get_write_setting(school)

        user_ad_info = self._find_ad_user(mo_values['cpr'], ad_dump)
        assert(len(user_ad_info) == 1)
        ad = user_ad_info[0]
        # Todo: Why is this not generated along with all other info in mo_values?
        mo_values['name_sam'] = '{} - {}'.format(mo_values['full_name'],
                                                 ad['SamAccountName'])
        mismatch = {}
        mismatch.update(self._cf(write_settings['level2orgunit_field'],
                                 mo_values['level2orgunit'], ad))
        mismatch.update(self._cf(write_settings['org_field'],
                                 mo_values['location'], ad))
        mismatch.update(self._cf('Name', mo_values['name_sam'], ad))
        mismatch.update(self._cf('DisplayName', mo_values['full_name'], ad))
        mismatch.update(self._cf('GivenName', mo_values['name'][0], ad))
        mismatch.update(self._cf('Surname', mo_values['name'][1], ad))
        mismatch.update(self._cf('EmployeeNumber',
                                 mo_values['employment_number'], ad))

        named_sync_fields = self.settings.get(
            'integrations.ad_writer.mo_to_ad_fields', {})

        for mo_field, ad_field in named_sync_fields.items():
            mismatch.update(self._cf(ad_field, mo_values[mo_field], ad))

        if mo_values.get('manager_cpr'):
            manager_ad_info = self._find_ad_user(mo_values['manager_cpr'], ad_dump)
            if not ad['Manager'] == manager_ad_info[0]['DistinguishedName']:
                mismatch['manager'] = (ad['Manager'],
                                       manager_ad_info[0]['DistinguishedName'])
                logger.info('Manager should be updated')
        return mismatch

    def sync_user(self, mo_uuid, ad_dump=None, sync_manager=True):
        """
        Sync MO information into AD
        """
        school = False  # TODO
        mo_values = self.read_ad_information_from_mo(
            mo_uuid, ad_dump=ad_dump, read_manager=sync_manager)

        if mo_values is None:
            return (False, 'No active engagments')

        if ad_dump is None:
            logger.debug('No AD information supplied, will look it up')
            user_sam = self._find_unique_user(mo_values['cpr'])
            # Todo, we could also add the compare logic here, but
            # the benifit will be max 40%
            mismatch = {'force re-sync': 'yes', 'manager': 'yes'}
        else:
            user_ad_info = self._find_ad_user(mo_values['cpr'], ad_dump)
            user_sam = user_ad_info[0]['SamAccountName']
            mismatch = self._sync_compare(mo_values, ad_dump)

        logger.debug('Sync compare: {}'.format(mismatch))

        if 'Name' in mismatch:
            logger.info('Rename user:')
            # Todo: This code is a duplicate of code 15 lines further down...
            rename_user_template = ad_templates.rename_user_template
            rename_user_string = rename_user_template.format(
                givenname=mo_values['name'][0],
                surname=mo_values['name'][1],
                sam_account_name=user_sam
            )
            rename_user_string = self.remove_redundant(rename_user_string)
            server_string = ''
            if self.all_settings['global'].get('servers') is not None:
                server_string = ' -Server {} '.format(
                    random.choice(self.all_settings['global']['servers'])
                )
            ps_script = (
                self._build_user_credential(school) +
                rename_user_string +
                server_string
            )
            logger.debug('Rename user, ps_script: {}'.format(ps_script))
            response = self._run_ps_script(ps_script)
            logger.debug('Response from sync: {}'.format(response))
            logger.debug('Wait for replication')
            # Todo: In principle we should ask all DCs, bu this will happen
            # very rarely, performance is not of great importance
            time.sleep(10)
            del mismatch['Name']

        if not mismatch:
            logger.info('Nothing to edit')
            return (True, 'Nothing to edit', mo_values['read_manager'])

        logger.info('Sync compare: {}'.format(mismatch))

        edit_user_string = template_create_user(
            cmd='Set-ADUser',
            context = {
                "mo_values": mo_values,
                "user_sam": user_sam,
            },
            settings = self.all_settings
        )
        edit_user_string = self.remove_redundant(edit_user_string)

#        edit_user_template = ad_templates.edit_user_template
#        replace_attributes = self._other_attributes(mo_values, user_sam,
#                                                    new_user=False)
#
#        edit_user_string = edit_user_template.format(
#            givenname=mo_values['name'][0],
#            surname=mo_values['name'][1],
#            sam_account_name=user_sam,
#            employment_number=mo_values['employment_number']
#        )
#        edit_user_string = self.remove_redundant(edit_user_string)
#        edit_user_string += replace_attributes

        server_string = ''
        if self.all_settings['global'].get('servers') is not None:
            server_string = ' -Server {} '.format(
                random.choice(self.all_settings['global']['servers'])
            )

        ps_script = (
            self._build_user_credential(school) +
            edit_user_string +
            server_string
        )
        logger.debug('Sync user, ps_script: {}'.format(ps_script))

        response = self._run_ps_script(ps_script)
        logger.debug('Response from sync: {}'.format(response))

        if sync_manager and 'manager' in mismatch:
            logger.info('Add manager')
            self.add_manager_to_user(user_sam=user_sam,
                                     manager_sam=mo_values['manager_sam'])

        return (True, 'Sync completed', mo_values['read_manager'])

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
        if mo_values is None:
            logger.error('Trying to create user with no engagements')
            raise NoPrimaryEngagementException

        all_names = mo_values['name'][0].split(' ') + [mo_values['name'][1]]
        sam_account_name = self.name_creator.create_username(all_names,
                                                             dry_run=dry_run)[0]

        existing_sam = self.get_from_ad(user=sam_account_name)
        existing_cpr = self.get_from_ad(cpr=mo_values['cpr'])
        if existing_sam:
            logger.error('SamAccount already in use: {}'.format(sam_account_name))
            raise SamAccountNameNotUnique(sam_account_name)
        if existing_cpr:
            logger.error('cpr already in use: {}'.format(mo_values['cpr']))
            raise CprNotNotUnique(mo_values['cpr'])

        create_user_string = template_create_user(
            context = {
                "mo_values": mo_values,
                "user_sam": sam_account_name,
            },
            settings = self.all_settings
        )
        create_user_string = self.remove_redundant(create_user_string)

#        create_user_template = ad_templates.create_user_template
#        other_attributes = self._other_attributes(mo_values, sam_account_name,
#                                                  new_user=True)
#
#        create_user_string = create_user_template.format(
#            givenname=mo_values['name'][0],
#            surname=mo_values['name'][1],
#            sam_account_name=sam_account_name,
#            employment_number=mo_values['employment_number']
#        )
#        create_user_string = self.remove_redundant(create_user_string)
#        create_user_string += other_attributes

        # Should this go to self._ps_boiler_plate()?
        server_string = ''
        if self.all_settings['global'].get('servers') is not None:
            server_string = ' -Server {} '.format(
                random.choice(self.all_settings['global']['servers'])
            )

        ps_script = (
            self._build_user_credential(school) +
            create_user_string +
            server_string +
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

    def enable_user(self, username, enable=True):
        """
        Enable or disable an AD account.
        :param username: SamAccountName of the account to be enabled or disabled
        :param enable: If True enable account, if False, disbale account
        """
        school = False  # TODO

        logger.info('Enable account: {}'.format(enable))
        format_rules = {'username': username}
        if enable:
            ps_script = self._build_ps(ad_templates.enable_user_template,
                                       school, format_rules)
        else:
            ps_script = self._build_ps(ad_templates.disable_user_template,
                                       school, format_rules)

        response = self._run_ps_script(ps_script)
        if not response:
            return (True, 'Account enabled or disabled')
        else:
            msg = 'Failed to update account!: {}'.format(response)
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
