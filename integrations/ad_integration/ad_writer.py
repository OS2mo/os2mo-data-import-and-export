# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod
import re
import json
import time
import random
import logging
import pathlib
import datetime

import click
from click_option_group import optgroup, RequiredMutuallyExclusiveOptionGroup
from jinja2 import Template

import ad_logger
import ad_templates
from ad_template_engine import template_powershell, prepare_field_templates
from utils import dict_map, dict_exclude, lower_list, dict_subset

from exporters.utils.lazy_dict import LazyDict, LazyEval, LazyEvalDerived
from integrations.ad_integration.ad_exceptions import CprNotNotUnique
from integrations.ad_integration.ad_exceptions import UserNotFoundException
from integrations.ad_integration.ad_exceptions import CprNotFoundInADException
from integrations.ad_integration.ad_exceptions import ReplicationFailedException
from integrations.ad_integration.ad_exceptions import NoActiveEngagementsException
from integrations.ad_integration.ad_exceptions import NoPrimaryEngagementException
from integrations.ad_integration.ad_exceptions import SamAccountNameNotUnique
from integrations.ad_integration.ad_exceptions import (
    ManagerNotUniqueFromCprException
)
from ad_common import AD
from user_names import CreateUserNames
from os2mo_helpers.mora_helpers import MoraHelper


logger = logging.getLogger("AdWriter")


def _random_password(length=12):
    password = ''
    for _ in range(0, length):
        password += chr(random.randrange(48, 127))
    return password


class MODataSource(ABC):

    @abstractmethod
    def read_user(self, uuid):
        """Read a user from MO using the provided uuid.

        Throws UserNotFoundException if the user cannot be found.

        Args:
            uuid: UUID for the user to lookup.

        Returns:
            dict: A dict with the users data.
        """
        raise NotImplementedError

    @abstractmethod
    def get_email_address(self, uuid):
        """Read a users email address using the provided uuid.

        Args:
            uuid: UUID for the user to lookup.

        Returns:
            dict: A dict of email address, potentially empty.
        """
        raise NotImplementedError

    @abstractmethod
    def find_primary_engagement(self, uuid):
        """Find the primary engagement for the provided uuid user.

        Args:
            uuid: UUID for the user to find primary engagement for.

        Returns:
            tuple(string, string, string, string):
                employment_number: Identifier for the engagement.
                title: Title of the job function for the engagement
                eng_org_unit: UUID of the organisation unit for the engagement
                eng_uuid: UUID of the found engagement
        """
        raise NotImplementedError

    @abstractmethod
    def get_manager_uuid(self, mo_user, eng_uuid):
        """Get UUID of the relevant manager for the user.

        Args:
            mo_user: MO user object, as returned by read_user.
            eng_uuid: UUID of the engagement, as returned by find_primary_engagement.

        Returns:
            str: A UUID string for the manager
        """
        raise NotImplementedError


class LoraCacheSource(MODataSource):
    """LoraCache implementation of the MODataSource interface."""

    def __init__(self, lc, lc_historic, mo_rest_source):
        self.lc = lc
        self.lc_historic = lc_historic
        self.mo_rest_source = mo_rest_source

    def read_user(self, uuid):
        if uuid not in self.lc.users:
            raise UserNotFoundException()

        lc_user = self.lc.users[uuid][0]
        mo_user = {
            'uuid': uuid,
            'name': lc_user['navn'],
            'surname': lc_user['efternavn'],
            'givenname': lc_user['fornavn'],
            'nickname': lc_user['kaldenavn'],
            'nickname_givenname': lc_user['kaldenavn_fornavn'],
            'nickname_surname': lc_user['kaldenavn_efternavn'],
            'cpr_no': lc_user['cpr']
        }
        return mo_user

    def get_email_address(self, uuid):
        mail_dict = {}
        for addr in self.lc.addresses.values():
            if addr[0]['user'] == uuid and addr[0]['scope'] == 'E-mail':
                mail_dict = addr[0]
        return dict_subset(mail_dict, ['uuid', 'value'])

    def find_primary_engagement(self, uuid):
        def filter_for_user(engagements):
            return filter(
                lambda eng: eng[0]['user'] == uuid, engagements
            )
        def filter_primary(engagements):
            return filter(
                lambda eng: eng[0]['primary_boolean'], engagements
            )

        user_engagements = list(filter_for_user(self.lc.engagements.values()))
        # No user engagements
        if not user_engagements:
            # But we may still have future engagements
            future_engagement = next(
                filter_for_user(self.lc_historic.engagements.values()), None
            )
            # We do not have any engagements at all
            if future_engagement is None:
                raise NoActiveEngagementsException()
            # We have future engagements, but LoraCache does not handle that.
            # Delegate to MORESTSource
            logger.info('Found future engagement')
            return self.mo_rest_source.find_primary_engagement(uuid)

        primary_engagement = next(filter_primary(user_engagements), None)
        if primary_engagement is None:
            raise NoPrimaryEngagementException('User: {}'.format(uuid))

        primary_engagement = primary_engagement[0]
        employment_number = primary_engagement['user_key']
        title = self.lc.classes[primary_engagement['job_function']]['title']
        eng_org_unit = primary_engagement['unit']
        eng_uuid = primary_engagement['uuid']
        return employment_number, title, eng_org_unit, eng_uuid

    def get_manager_uuid(self, mo_user, eng_uuid):
        def org_uuid_parent(org_uuid):
            parent_uuid = self.lc.units[org_uuid][0]['parent']
            return parent_uuid

        def org_uuid_to_manager(org_uuid):
            org_unit = self.lc.units[org_uuid][0]
            manager_uuid = self.lc.managers[
                org_unit['acting_manager_uuid']
            ][0]['user']
            return manager_uuid

        try:
            # Compatibility to mimic MORESTSource behaviour
            # MORESTSource does an engagement lookup in the present, using
            # the org uuid from that and fails if it doesn't find anything
            engagement = self.lc.engagements[eng_uuid][0]
            eng_org_unit = engagement['unit']
            manager_uuid = org_uuid_to_manager(eng_org_unit)
            if manager_uuid is None:
                raise Exception("Unable to find manager")
            # We found a manager directly
            if manager_uuid != mo_user['uuid']:
                return manager_uuid
            # Self manager, find a manager above us, if possible
            parent_uuid = org_uuid_parent(eng_org_unit)
            while manager_uuid == mo_user['uuid']:
                if parent_uuid is None:
                    return manager_uuid
                manager_uuid = org_uuid_to_manager(parent_uuid)
                parent_uuid = org_uuid_parent(parent_uuid)
            return manager_uuid
        except KeyError as exp:
            return None


class MORESTSource(MODataSource):
    """MO REST implementation of the MODataSource interface."""

    def __init__(self, settings):
        self.helper = MoraHelper(
            hostname=settings['global']['mora.base'], use_cache=False
        )

    def read_user(self, uuid):
        mo_user = self.helper.read_user(user_uuid=uuid)
        if 'uuid' not in mo_user:
            raise UserNotFoundException()
        else:
            assert(mo_user['uuid'] == uuid)
        exclude_fields = ['org', 'user_key']
        mo_user = dict_exclude(mo_user, exclude_fields)
        return mo_user

    def get_email_address(self, uuid):
        mail_dict = self.helper.get_e_address(uuid, scope='EMAIL')
        return dict_subset(mail_dict, ['uuid', 'value'])

    def find_primary_engagement(self, uuid):
        def filter_primary(engagements):
            return filter(
                lambda eng: eng['is_primary'], engagements
            )

        user_engagements = self.helper.read_user_engagement(
            uuid, calculate_primary=True, read_all=True, skip_past=True
        )
        if not user_engagements:
            raise NoActiveEngagementsException()

        primary_engagement = next(filter_primary(user_engagements), None)
        if primary_engagement is None:
            raise NoPrimaryEngagementException('User: {}'.format(uuid))

        employment_number = primary_engagement['user_key']
        title = primary_engagement['job_function']['name']
        eng_org_unit = primary_engagement['org_unit']['uuid']
        eng_uuid = primary_engagement['uuid']
        return employment_number, title, eng_org_unit, eng_uuid

    def get_manager_uuid(self, mo_user, eng_uuid):
        try:
            manager = self.helper.read_engagement_manager(eng_uuid)
            manager_uuid = manager['uuid']
            return manager_uuid
        except KeyError:
            return None


class ADWriter(AD):
    def __init__(self, lc=None, lc_historic=None, occupied_names=None, **kwargs):
        super().__init__(**kwargs)
        self.opts = dict(**kwargs)

        self.settings = self.all_settings

        # Setup datasource for getting MO data.
        # TODO: Create a factory instead of this hackery?
        # Default to using MORESTSource as data source
        self.datasource = MORESTSource(self.settings)
        # Use LoraCacheSource if LoraCache is provided
        if lc and lc_historic:
            self.datasource = LoraCacheSource(lc, lc_historic, self.datasource)
        # NOTE: These should be eliminated when all uses are gone
        # NOTE: Once fully utilized, tests should be able to just implement a
        #       MODataSource for all their mocking needs.
        self.lc = lc
        self.lc_historic = lc_historic
        self.helper = MoraHelper(hostname=self.settings['global']['mora.base'],
                                 use_cache=False)

        self._init_name_creator(occupied_names)

    def _init_name_creator(self, occupied_names):
        self.name_creator = CreateUserNames(occupied_names)
        if occupied_names is None:
            logger.info('Reading occupied names')
            self.name_creator.populate_occupied_names(**self.opts)
        logger.info('Done reading occupied names')

    def _get_write_setting(self):
        if not self.all_settings['primary_write']:
            msg = 'Trying to enable write access with broken settings.'
            logger.error(msg)
            raise Exception(msg)
        return self.all_settings['primary_write']

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
        return self.datasource.read_user(uuid)

    def _find_ad_user(self, cpr, ad_dump):
        ad_info = []
        if ad_dump is not None:
            for user in ad_dump:
                if user.get(self.all_settings['primary']['cpr_field']) == cpr:
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
        # TODO: Convert to datasource
        write_settings = self._get_write_setting()

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
                if write_settings['level2orgunit_type'] in (
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
                if write_settings['level2orgunit_type'] in (
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
        # TODO: Convert to datasource
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
        # TODO: Convert to datasource
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
            'location': 'Viborg Kommune\\Forvalting\\Enhed\\',
            'level2orgunit: 'Beskæftigelse, Økonomi & Personale',
            'manager_sam': 'DMILL'
        }
        """
        logger.info('Read information for {}'.format(uuid))
        try:
            employment_number, title, eng_org_unit, eng_uuid = self.datasource.find_primary_engagement(uuid)
        except NoActiveEngagementsException:
            logger.info('No active engagements found')
            return None

        def split_addresses(addresses):
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
            return {
                'postal_code': postal_code,
                'city': city,
                'streetname': streetname,
            }

        def read_manager_uuid(mo_user, eng_uuid):
            manager_uuid = self.datasource.get_manager_uuid(
                mo_user, eng_uuid
            )
            if manager_uuid is None:
                logger.info('No managers found')
            return manager_uuid

        def read_manager_mail(manager_uuid):
            manager_mail_dict = self.datasource.get_email_address(manager_uuid)
            if manager_mail_dict:
                return manager_mail_dict['value']
            return None

        def read_manager_sam(manager_cpr):
            try:
                manager_ad_info = self._find_ad_user(
                    cpr=manager_cpr, ad_dump=ad_dump
                )
            except CprNotFoundInADException:
                manager_ad_info = []

            if len(manager_ad_info) == 1:
                return manager_ad_info[0]['SamAccountName']
            else:
                msg = 'Searching for {}, found in AD: {}'
                logger.debug(msg.format(manager_info['name'], manager_ad_info))
                raise ManagerNotUniqueFromCprException()
            return None

        # NOTE: Underscore fields should not be read
        mo_values: LazyDict = LazyDict({
            # Raw information
            "uuid": uuid,

            # Engagement information
            "employment_number": employment_number,
            "title": title,
            "unit_uuid": eng_org_unit,
            "_eng_uuid": eng_uuid,

            'end_date': LazyEvalDerived(
                lambda uuid: self._find_end_date(uuid)
            ),

            # Lazy MO User and associated fields
            '_mo_user': LazyEvalDerived(
                lambda uuid: self._read_user(uuid)
            ),
            'name': LazyEvalDerived(
                lambda _mo_user: (
                    _mo_user['givenname'], _mo_user['surname']
                )
            ),
            'full_name': LazyEvalDerived(
                lambda name: '{} {}'.format(*name)
            ),
            'nickname': LazyEvalDerived(
                lambda _mo_user: (
                    _mo_user['nickname_givenname'], _mo_user['nickname_surname']
                )
            ),
            'full_nickname': LazyEvalDerived(
                lambda nickname: '{} {}'.format(nickname)
            ),
            'cpr_no': LazyEvalDerived(
                lambda _mo_user: _mo_user['cpr_no']
            ),

            # Lazy Unit and associated fields
            "_unit": LazyEvalDerived(
                lambda unit_uuid: self._find_unit_info(unit_uuid)
            ),
            'unit': LazyEvalDerived(
                lambda _unit: _unit["name"]
            ),
            'unit_user_key': LazyEvalDerived(
                lambda _unit: _unit["user_key"]
            ),
            'location': LazyEvalDerived(
                lambda _unit: _unit["location"]
            ),
            'level2orgunit': LazyEvalDerived(
                lambda _unit: _unit["level2orgunit"]
            ),

            # Lazy addresses and associated fields
            "_addresses": LazyEvalDerived(
                lambda unit_uuid: self._read_user_addresses(unit_uuid)
            ),
            "_parsed_addresses": LazyEvalDerived(
                lambda _addresses: split_addresses(_addresses)
            ),
            'unit_postal_code': LazyEvalDerived(
                lambda _parsed_addresses: _parsed_addresses['postal_code']
            ),
            'unit_city': LazyEvalDerived(
                lambda _parsed_addresses: _parsed_addresses['city']
            ),
            'unit_streetname': LazyEvalDerived(
                lambda _parsed_addresses: _parsed_addresses['streetname']
            ),
            'unit_public_email': LazyEvalDerived(
                lambda _addresses: _addresses['unit_public_email']
            ),
            'unit_secure_email': LazyEvalDerived(
                lambda _addresses: _addresses['unit_secure_email']
            ),

            # Manager stuff
            "_manager_uuid": LazyEval(
                lambda key, dictionary: (
                    read_manager_uuid(
                        dictionary["_mo_user"], dictionary["_eng_uuid"]
                    ) if read_manager else None
                )
            ),
            "_manager_mo_user": LazyEvalDerived(
                lambda _manager_uuid: self._read_user(_manager_uuid) if _manager_uuid else {}
            ),
            "manager_name": LazyEvalDerived(
                lambda _manager_mo_user: _manager_mo_user.get("name")
            ),
            "manager_cpr": LazyEvalDerived(
                lambda _manager_mo_user: _manager_mo_user.get("cpr_no")
            ),
            "manager_mail": LazyEvalDerived(
                lambda _manager_uuid: read_manager_mail(_manager_uuid) if _manager_uuid else None
            ),
            "manager_sam": LazyEvalDerived(
                lambda manager_cpr: read_manager_sam(manager_cpr) if manager_cpr else None
            ),
            "read_manager": LazyEvalDerived(
                lambda _manager_uuid: bool(_manager_uuid)
            ),
        })
        return mo_values

    def add_manager_to_user(self, user_sam, manager_sam):
        """
        Mark an existing AD user as manager for an existing AD user.
        :param user_sam: SamAccountName for the employee.
        :param manager_sam: SamAccountName for the manager.
        """
        format_rules = {'user_sam': user_sam, 'manager_sam': manager_sam}
        ps_script = self._build_ps(ad_templates.add_manager_template, format_rules)

        response = self._run_ps_script(ps_script)
        return response is {}

    def _cf(self, ad_field, value, ad):
        logger.info('Check AD field: {}'.format(ad_field))
        mismatch = {}
        if value is None:
            msg = 'Value for {} is None-type replace to string None'
            logger.debug(msg.format(ad_field))
            value = 'None'
        if ad.get(ad_field) != value:
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
        write_settings = self._get_write_setting()

        user_ad_info = self._find_ad_user(mo_values['cpr'], ad_dump)
        assert(len(user_ad_info) == 1)
        ad = user_ad_info[0]
        user_sam = ad['SamAccountName']
        # TODO: Why is this not generated along with all other info in mo_values?
        mo_values['name_sam'] = '{} - {}'.format(mo_values['full_name'],
                                                 ad['SamAccountName'])

        fields = prepare_field_templates("Set-ADUser", settings=self.all_settings)

        def to_lower(string):
            return string.lower()

        ad = dict_map(ad, key_func=to_lower)
        fields = dict_map(fields, key_func=to_lower)

        never_compare = lower_list(['Credential', 'Manager'])
        fields = dict_exclude(fields, never_compare)

        context = {
            "mo_values": mo_values,
            "user_sam": user_sam,
        }
        def render_field_template(template):
            return Template(template.strip('"')).render(**context)

        # Build context and render template to get comparision value
        # NOTE: This results in rendering the template twice, once here and
        #       once inside the powershell render call.
        #       We should probably restructure this, such that we only render
        #       the template once, potentially rendering a dict of results.
        # TODO: Make the above mentioned change.
        fields = dict_map(fields, value_func=render_field_template)
        mismatch = {}
        for ad_field, rendered_value in fields.items():
            mismatch.update(self._cf(ad_field, rendered_value, ad))

        if mo_values.get('manager_cpr'):
            manager_ad_info = self._find_ad_user(mo_values['manager_cpr'], ad_dump)
            if not ad['manager'] == manager_ad_info[0]['DistinguishedName']:
                mismatch['manager'] = (ad['manager'],
                                       manager_ad_info[0]['DistinguishedName'])
                logger.info('Manager should be updated')
        return mismatch

    def sync_user(self, mo_uuid, ad_dump=None, sync_manager=True):
        """
        Sync MO information into AD
        """
        mo_values = self.read_ad_information_from_mo(
            mo_uuid, ad_dump=ad_dump, read_manager=sync_manager
        )

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

        if 'name' in mismatch:
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
                self._build_user_credential() +
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
            del mismatch['name']

        if not mismatch:
            logger.info('Nothing to edit')
            return (True, 'Nothing to edit', mo_values['read_manager'])

        logger.info('Sync compare: {}'.format(mismatch))

        edit_user_string = template_powershell(
            cmd='Set-ADUser',
            context = {
                "mo_values": mo_values,
                "user_sam": user_sam,
            },
            settings = self.all_settings
        )
        edit_user_string = self.remove_redundant(edit_user_string)

        server_string = ''
        if self.all_settings['global'].get('servers') is not None:
            server_string = ' -Server {} '.format(
                random.choice(self.all_settings['global']['servers'])
            )

        ps_script = (
            self._build_user_credential() +
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
        # TODO: Implement dry_run

        bp = self._ps_boiler_plate()
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

        create_user_string = template_powershell(
            context = {
                "mo_values": mo_values,
                "user_sam": sam_account_name,
            },
            settings = self.all_settings
        )
        create_user_string = self.remove_redundant(create_user_string)

        # Should this go to self._ps_boiler_plate()?
        server_string = ''
        if self.all_settings['global'].get('servers') is not None:
            server_string = ' -Server {} '.format(
                random.choice(self.all_settings['global']['servers'])
            )

        ps_script = (
            self._build_user_credential() +
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

        format_rules = {'username': username, 'password': password}
        ps_script = self._build_ps(ad_templates.set_password_template, format_rules)
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

        logger.info('Enable account: {}'.format(enable))
        format_rules = {'username': username}
        if enable:
            ps_script = self._build_ps(ad_templates.enable_user_template,
                                       format_rules)
        else:
            ps_script = self._build_ps(ad_templates.disable_user_template,
                                       format_rules)

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
                                   format_rules=format_rules)

        response = self._run_ps_script(ps_script)
        # TODO: Should we make a read to confirm the user is gone?
        if not response:
            return (True, 'User deleted')
        else:
            logger.error('Failed to delete account!: {}'.format(response))
            return (False, 'Failed to delete')


@click.command()
@optgroup.group("Action", cls=RequiredMutuallyExclusiveOptionGroup)
@optgroup.option(
    '--create-user-with-manager',
    help='Create a new user in AD, also assign a manager',
)
@optgroup.option(
    '--create-user',
    help='Create a new user in AD, do not assign a manager',
)
@optgroup.option(
    '--sync-user',
    help='Sync relevant fields from MO to AD',
)
@optgroup.option('--delete-user')
@optgroup.option('--read-ad-information')
@optgroup.option('--add-manager-to-user', nargs=2, type=str)
def cli(**args):
    """
    Command line interface for the AD writer class.
    """

    ad_writer = ADWriter()

    if args.get('create_user_with_manager'):
        print('Create_user_with_manager:')
        status = ad_writer.create_user(
            args['create_user_with_manager'], create_manager=True
        )
        # TODO: execute custom script? Or should this be done in
        # two steps.
        print(status[1])

    if args.get('create_user'):
        print('Create user, no link to manager:')
        status = ad_writer.create_user(
            args['create_user'], create_manager=False
        )
        print(status[1])

    if args.get('sync_user'):
        print('Sync MO fields to AD')
        status = ad_writer.sync_user(args['sync_user'])
        print(status[1])

    if args.get('delete_user'):
        print('Deleting user:')
        status = ad_writer.delete_user(args['delete_user'])
        print(status[1])

    if args.get('read_ad_information'):
        print('AD information on user:')
        sam = args['read_ad_information']
        user = ad_writer.get_from_ad(user=sam)
        if not user:
            print('User not found')
        else:
            for key, value in sorted(user[0].items()):
                print('{}: {}'.format(key, value))

    if args.get('add_manager_to_user'):
        manager, user = args['add_manager_to_user']
        print('{} is now set as manager for {}'.format(manager, user))
        ad_writer.add_manager_to_user(manager_sam=manager, user_sam=user)

    # TODO: Enable a user, including setting a random password
    # ad_writer.set_user_password('MSLEG', _random_password())
    # ad_writer.enable_user('OBRAP')


if __name__ == '__main__':
    ad_logger.start_logging('ad_writer.log')
    cli()
