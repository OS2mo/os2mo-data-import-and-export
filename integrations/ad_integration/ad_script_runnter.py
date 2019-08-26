import os
from ad_writer import ADWriter
from os2mo_helpers.mora_helpers import MoraHelper

MORA_BASE = os.environ.get('MORA_BASE', None)

AD_NAME = 'Active Directory'
SCHOOL_AD_NAME = 'Active Directory - School'
PRIMARY_ENGAGEMENT = 'd59a5bca-a0ba-403b-ad92-f62b799bf249'


class ADScriptRunner(object):
    """
    Run a power shell script after having inserted suitable parameters  from MO.
    """

    def __init__(self):
        self.helper = MoraHelper(hostname=MORA_BASE, use_cache=False)
        self.ad_writer = ADWriter()

        # TODO: Add logic to check for school AD
        self.ad_name = AD_NAME
        self.primary_engagement_uuid = PRIMARY_ENGAGEMENT

    def _read_script_template(self, script_name):
        """
        Read a script template
        """
        pass

    def _read_mo_user(self, uuid):
        """
        Read the parameters for the script from MO
        :param uuid: MO uuid for the releant user
        """

        user_info = {}
        # From the specification, we need to read these parameters:
        # Fornavn, Efternavn, cpr nummer, SamAccountName,
        # navn og uuid på orgenhed for primær ansættelse

        # Leders email, Leders navn
        mo_person = self.helper.read_user(user_uuid=uuid)

        # Primary user information
        user_info['cpr'] = mo_person['cpr_no']
        user_info['fornavn'] = mo_person['givenname']
        user_info['efternavn'] = mo_person['surname']

        # AD username
        sam_account = self.helper.get_e_username(uuid, self.ad_name)
        user_info['sam_account'] = sam_account

        # Find primary engagement, if no primary is found, None is returned
        user_info['ou_uuid'] = None
        user_info['ou_name'] = None
        engagements = self.helper.read_user_engagement(uuid, only_primary=True)
        for engagement in engagements:
            if engagement['engagement_type']['uuid'] == self.primary_engagement_uuid:
                user_info['ou_uuid'] = engagement['org_unit']['uuid']
        if user_info['ou_uuid'] is not None:
            ou_name = self.helper.read_ou(user_info['ou_uuid'])['name']
            user_info['ou_name'] = ou_name

        # Find manager, ie. the manager of the unit of the primary engagement
        user_info['manager_name'] = None
        user_info['manager_email'] = None

        # No managers in current dataset
        return user_info

if __name__ == '__main__':
    runner = ADScriptRunner()
    # print(runner._read_mo_user(uuid=''))
