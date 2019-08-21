import time
import json
import pickle
import logging
import hashlib
from ad_common import AD
# import read_ad_conf_settings
from pathlib import Path

logger = logging.getLogger("AdReader")


class ADWriter(AD):
    def __init__(self):
        super().__init__()
    


    def create_user(self):
        school = False # TODO

        create_user_template = 'New-ADUser -Name "TestMO005" -SamAccountName "TestMO005" -OtherAttributes @{"extensionattribute1"="1111110101";"hkstsuuid"="5826074e-66c3-4100-8a00-000001510001"} '
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
        response = self._run_ps_script(ps_script)
        print()
        print(response)

if __name__ == '__main__':
    ad_writer = ADWriter()

    ad_writer.create_user()

