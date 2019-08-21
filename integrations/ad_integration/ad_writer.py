import logging
from ad_common import AD
# import read_ad_conf_settings

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

    def create_user(self, name, mo_uuid, cpr):
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

        create_user_template = """
        New-ADUser
        -Name "{} {}"
        -GivenName "{}"
        -Surname "{}"
        -SamAccountName "AseAsesen2"
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
        print(ps_script)
        response = self._run_ps_script(ps_script)
        print()
        print(response)

    def create_user_test(self):
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
    ad_writer = ADWriter()

    # ad_writer.create_user_test()
    ad_writer.create_user(
        name=('Daw', 'Dawsen'),
        mo_uuid='5826074e-66c3-4100-8a00-000001510001',
        cpr='1111110101'
    )
