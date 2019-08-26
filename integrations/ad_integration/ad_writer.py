import logging
from ad_common import AD
from user_names import CreateUserNames

logger = logging.getLogger("AdWriter")


def remove_redundant(text):
    text = text.replace('\n', '')
    text = text.replace('\r', '')
    while text.find('  ') > -1:
        text = text.replace('  ', ' ')
    return text


class ADWriter(AD):
    def __init__(self):
        super().__init__()

        name_creator = CreateUserNames(occupied_names=set())
        logger.info('Reading occupied names')
        name_creator.populate_occupied_names()
        logger.info('Done reading occupied names')
        
    def create_user(self, name, mo_uuid, cpr, dry_run=False):
        """
        Create an AD user
        :param name: Tuple with (givenname, surname)
        """
        school = False  # TODO

        settings = self._get_setting(school)
        server = ''
        if settings['server']:
            server = ' -Server {} '.format(settings['server'])

        path = ' -Path "{}" '.format(settings['search_base'])

        credentials = ' -Credential $usercredential'

        # TODO: Create SamAccountName
        print(name[0].split(' ') + name[1])
        exit()
        
        # TODO: Double check that the SamAccountName is available
        
        create_user_template = """
        New-ADUser
        -Name "{} {}"
        -GivenName "{}"
        -Surname "{}"
        -SamAccountName "AseAsesen9"
        -OtherAttributes @{{"extensionattribute1"="{}";"hkstsuuid"="{}"}}"""
        create_user_string = create_user_template.format(
            name[0],
            name[1],
            name[0],
            name[1],
            cpr,
            mo_uuid
        )
        create_user_string = remove_redundant(create_user_string)

        ps_script = (
            self._build_user_credential(school) +
            create_user_string +
            server +
            path +
            credentials
        )
        response = self._run_ps_script(ps_script)
        # TODO: Return the new SamAccoutName if succss
        if response is {}:
            return True
        else:
            return response

    def enable_user(self, username):
        """
        Disable an AD account.
        :param username: SamAccountName of the account to be disabled
        """
        school = False  # TODO

        settings = self._get_setting(school)
        server = ''
        if settings['server']:
            server = ' -Server {} '.format(settings['server'])

        path = ' -Path "{}" '.format(settings['search_base'])

        credentials = ' -Credential $usercredential'
        # delete_user_template = """ Remove-ADUser  -Identity "{}"   """.format(username)

        enable_user_template = """ Get-ADUser -Filter 'SamAccountName -eq \"{}\"'  """.format(username)
        enable_user_template += credentials + ' ' + server + ' | Enable-ADAccount'
        ps_script = (
            self._build_user_credential(school) +
            enable_user_template +
            server +
            # path +
            credentials
        )
        print(ps_script)
        # exit()
        print()
        response = self._run_ps_script(ps_script)
        print(response)


    def delete_user(self, username):
        """
        Delete an AD account. Only to be used for service purpose, actual
        AD integration should never delete a user, but only mark it for
        deletetion.
        :param username: SamAccountName of the account to be deleted
        """
        school = False  # TODO

        settings = self._get_setting(school)
        server = ''
        if settings['server']:
            server = ' -Server {} '.format(settings['server'])

        path = ' -Path "{}" '.format(settings['search_base'])

        credentials = ' -Credential $usercredential'
        # delete_user_template = """ Remove-ADUser  -Identity "{}"   """.format(username)

        delete_user_template = """ Get-ADUser -Filter 'SamAccountName -eq \"{}\"'  """.format(username)
        delete_user_template += credentials + ' ' + server + ' | Remove-ADUser'
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

    
    def create_user_test(self):
        """ Test function, only used for primitie write tests """
        school = False  # TODO

        create_user_template = (
            'New-ADUser -Name "TestMO005" -SamAccountName "TestMO005" ' +
            '-OtherAttributes @{"extensionattribute1"="1111110101";' +
            '"hkstsuuid"="5826074e-66c3-4100-8a00-000001510001"} '
        )
        settings = self._get_setting(school)
        server = ''
        if settings['server']:
            server = ' -Server {} '.format(settings['server'])

        path = ' -Path "{}" '.format(settings['search_base'])

        credentials = ' -Credential $usercredential'
        ps_script = (
            self._build_user_credential(school) +
            create_user_template +
            server +
            path +
            credentials
        )
        print(ps_script)
        # response = self._run_ps_script(ps_script)
        # print()
        # print(response)


if __name__ == '__main__':
    name_creator = CreateUserNames(occupied_names=set())
    name_creator.populate_occupied_names()
    
    # ad_writer = ADWriter()
    # user = ad_writer.get_from_ad(user='AseAsesen1')
    # print(user[0]['Enabled'])

    # This will not work until we make proper passwords
    # print(ad_writer.enable_user('AseAsesen1'))

    # This does not work for unkown reasons
    # ad_writer.delete_user('AseAsesen1')


    # print(ad_writer.delte_user('AseAsesen2'))
    # ad_writer.create_user_test()
    # ad_writer.create_user(
    #     name=('Daaaw', 'Dawwwsen'),
    #     mo_uuid='5826074e-66c3-4100-8a00-000001510001',
    #     cpr='1111110101'
    # )
