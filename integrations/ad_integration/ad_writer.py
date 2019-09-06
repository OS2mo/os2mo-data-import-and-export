import os
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


if MORA_BASE is None or PRIMARY_ENGAGEMENT_TYPE is None:
    msg = 'Configuration error: MORA_BASE: {}, PRIMARY_ENGAGEMENT_TYPE: {}'
    raise Exception(msg.format(MORA_BASE, PRIMARY_ENGAGEMENT_TYPE))


def remove_redundant(text):
    text = text.replace('\n', '')
    text = text.replace('\r', '')
    while text.find('  ') > -1:
        text = text.replace('  ', ' ')
    return text


def _random_password(length=12):
    password = ''
    for _ in range(0, length):
        password += chr(random.randrange(33, 127))
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

    def _ps_boiler_plate(self, school):
        """
        Boiler plate that needs to go into all PowerShell code.
        """
        settings = self._get_setting(school)
        server = ''
        if settings['server']:
            server = ' -Server {} '.format(settings['server'])

        path = ''
        if settings['search_base']:
            path = ' -Path "{}" '.format(settings['search_base'])
        credentials = ' -Credential $usercredential'

        boiler_plate = {
            'server': server,
            'path': path,
            'credentials': credentials,
            'complete': server + path + credentials
        }
        return boiler_plate

    def _build_ps(self, ps_script, school, format_rules):
        """
        Return the standard code need to execute a power shell script from a
        template.
        """
        formatted_script = ps_script.format(**format_rules)
        finished_ps_script = (
            self._build_user_credential(school) +
            remove_redundant(formatted_script)
        )
        return finished_ps_script

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
            'location': 'Viborg Kommune/Beskæftigelse, Økonomi & Personale/',
            'unit': 'It-strategisk team',
            'managerSAM': 'DMILL'
        }
        """
        # TODO: We need some kind of error handling here
        mo_user = self.helper.read_user(user_uuid=uuid)
        if 'uuid' not in mo_user:
            raise ad_exceptions.UserNotFoundException
        else:
            assert(mo_user['uuid'] == uuid)

        engagements = self.helper.read_user_engagement(uuid)

        found_primary = False
        for engagement in engagements:
            if engagement['engagement_type']['uuid'] == PRIMARY_ENGAGEMENT_TYPE:
                found_primary = True
                employment_number = engagement['user_key']
                title = engagement['job_function']['name']

        if not found_primary:
            raise ad_exceptions.NoPrimaryEngagementException('User: {}'.format(uuid))

        unit_info = self.helper.read_ou(engagement['org_unit']['uuid'])
        unit = unit_info['name']
        location = unit_info['location']
        if location == '':
            location = 'root'

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
            'unit': unit,
            'managerSAM': manager_sam
        }
        return mo_values

    def create_user(self, mo_uuid, dry_run=False):
        """
        Create an AD user
        :param name: Tuple with (givenname, surname)
        """
        school = False  # TODO
        # TODO: Implement dry_run

        write_settings = self._get_write_setting(school)
        read_settings = self._get_setting(school)
        server = ''
        if read_settings['server']:
            server = ' -Server {} '.format(read_settings['server'])

        path = ' -Path "{}" '.format(read_settings['search_base'])
        credentials = ' -Credential $usercredential'

        mo_values = self.read_ad_informaion_from_mo(mo_uuid)
        all_names = mo_values['name'][0].split(' ') + [mo_values['name'][1]]
        sam_account_name = self.name_creator.create_username(all_names,
                                                             dry_run=dry_run)[0]

        create_user_template = ad_templates.create_user_template

        other_attributes = ' -OtherAttributes @{'
        other_attributes_fields = [
            (write_settings['uuid_field'], mo_values['uuid']),
            (write_settings['cpr_field'], mo_values['cpr']),
            (write_settings['unit_field'], mo_values['unit'].replace('&', 'og')),
            (write_settings['org_field'], mo_values['location'].replace('&', 'og'))
        ]
        for field in other_attributes_fields:
            other_attributes += '"{}"="{}";'.format(field[0], field[1])
        other_attributes += '}'

        full_name = '{} {}'.format(mo_values['name'][0], mo_values['name'][1])
        create_user_string = create_user_template.format(
            full_name,
            sam_account_name,
            full_name,
            mo_values['name'][0],
            mo_values['name'][1],
            sam_account_name,
            mo_values['employment_number']
        )
        create_user_string = remove_redundant(create_user_string)
        create_user_string += other_attributes

        ps_script = (
            self._build_user_credential(school) +
            create_user_string +
            server +
            path +
            credentials
        )
        response = self._run_ps_script(ps_script)
        print(ps_script)
        print()
        print(response)
        # TODO:
        # Efter oprettelsen af brugeren, skal vi efterfølgende lave endnu et kald
        # til PowerShell for at oprette en relation til lederen.

        if response is {}:
            return sam_account_name
        else:
            return response

    def set_user_password(self, username, password):
        """
        Set a password for a user.
        :param username: SamAccountName for the user.
        :param password: The password to assign to the user.
        :return: True if success, otherwise False
        """
        school = False  # TODO

        bp = self._ps_boiler_plate(school)
        format_rules = {'username': username, 'credentials': bp['credentials'],
                        'password': password}
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

        bp = self._ps_boiler_plate(school)
        format_rules = {'username': username, 'credentials': bp['credentials']}
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
        school = False  # TODO

        # ad_user = self.get_from_ad(cpr=manager_cpr)
        ad_user = self.get_from_ad(user=username)[0]
        dn = ad_user['DistinguishedName']

        settings = self._get_setting(school)
        server = ''
        if settings['server']:
            server = ' -Server {} '.format(settings['server'])

        path = ' -Path "{}" '.format(settings['search_base'])

        credentials = ' -Credential $usercredential'
        delete_user_template = """ Remove-ADUser  -Identity "{}"   """.format(dn)

        # delete_user_template = """ Get-ADUser -Filter 'SamAccountName -eq \"{}\"'  """.format(username)
        # delete_user_template += credentials + ' ' + server + ' | Remove-ADUser'
        ps_script = (
            self._build_user_credential(school) +
            delete_user_template +
            server +
            # path +
            credentials
        )
        print(ps_script)
        # exit()
        print()
        response = self._run_ps_script(ps_script)
        print(response)


if __name__ == '__main__':
    ad_writer = ADWriter()

    # ad_writer.read_ad_informaion_from_mo(uuid)
    # print(ad_writer.read_ad_informaion_from_mo('7ccbd9aa-b163-4a5a-bf2f-0e6f41f6ebc0'))

    # ad_writer.create_user(uuid)

    # user = ad_writer.get_from_ad(user='AseAsesen1')
    # print(user[0]['Enabled'])

    # This will not work until we make proper passwords

    # print(ad_writer.get_from_ad(user='MLEEG')[0]['Enabled'])

    ad_writer.set_user_password('MLEEG', _random_password())
    # print(ad_writer.enable_user('MSLEG'))

    # This does not work for unknown reasons
    # ad_writer.delete_user('MLEGO')

    # print(ad_writer.delte_user('AseAsesen2'))
    # ad_writer.create_user_test()
    # ad_writer.create_user(
    #     name=('Daaaw', 'Dawwwsen'),
    #     mo_uuid='5826074e-66c3-4100-8a00-000001510001',
    #     cpr='1111110101'
    # )
