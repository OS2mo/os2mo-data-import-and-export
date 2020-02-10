import uuid
import json
import pickle
import pathlib
import hashlib
import logging
import argparse
import datetime

import requests

from os2mo_helpers.mora_helpers import MoraHelper
from integrations.ad_integration import payloads
from integrations.ad_integration.ad_reader import ADParameterReader

# Set up a real logger!
logger = logging.getLogger("ImportADGroup")

LOG_LEVEL = logging.DEBUG
LOG_FILE = 'external_ad_users.log'

for name in logging.root.manager.loggerDict:
    if name in ('ImportADGroup', 'AdReader', 'mora-helper', 'AdCommon'):
        logging.getLogger(name).setLevel(LOG_LEVEL)
    else:
        logging.getLogger(name).setLevel(logging.ERROR)

logging.basicConfig(
    format='%(levelname)s %(asctime)s %(name)s %(message)s',
    level=LOG_LEVEL,
    filename=LOG_FILE
)


class ADMOImporter(object):
    def __init__(self):
        cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
        if not cfg_file.is_file():
            raise Exception('No setting file')
        self.settings = json.loads(cfg_file.read_text())

        self.helper = MoraHelper(hostname=self.settings['mora.base'],
                                 use_cache=False)
        try:
            self.org_uuid = self.helper.read_organisation()
        except requests.exceptions.RequestException as e:
            logger.error(e)
            print(e)
            exit()

        self.ad_reader = ADParameterReader()

        # This will be populated by _find_or_create_unit_and_classes
        self.uuids = None

        # All relevant AD users, populated by _update_list_of_external_users_in_ad
        self.users = None

    # This function also exists i opus_helpers
    def _generate_uuid(self, value):
        """
        Generate a predictable uuid based on org name and a unique value.
        """
        base_hash = hashlib.md5(self.settings['municipality.name'].encode())
        base_digest = base_hash.hexdigest()
        base_uuid = uuid.UUID(base_digest)

        combined_value = (str(base_uuid) + str(value)).encode()
        value_hash = hashlib.md5(combined_value)
        value_digest = value_hash.hexdigest()
        value_uuid = uuid.UUID(value_digest)
        return value_uuid

    # Similar versions exists in opus-diff-import and sd_changed_at
    def _add_klasse_to_lora(self, bvn, navn, facet_uuid):
        """
        Helper to add a Klasse to LoRa.
        :param bvn: BrugerVendtNøgle for the Klasse.
        :param navn: Titel for the klasse.
        :param facet_uuid: The facet that the Klasse will belong to.
        :return uuid of the Klasse.
        """
        klasse_uuid = self._generate_uuid(bvn)
        msg = 'Adding Klasse: {}, bvn: {}, uuid: {}'
        logger.debug(msg.format(navn, bvn, klasse_uuid))
        payload = payloads.klasse(bvn, navn, self.org_uuid, facet_uuid)
        url = '{}/klassifikation/klasse/{}'
        response = requests.put(
            url=url.format(self.settings['mox.base'], klasse_uuid),
            json=payload
        )
        assert response.status_code == 200
        return response.json()

    def _fc_klasse(self, bvn, navn, facet):
        """
        Find or create Klasse.
        Return the uuid of a given Klasse. If it does not exist, it will be created.
        :param klassenavn: String with the user key of the Klasse.
        :param facet: String with the name of the Facet for the Klasse.
        :return: uuid of the Klasse
        """
        url = self.settings['mox.base'] + '/klassifikation/klasse?bvn=' + bvn
        response = requests.get(url)
        response.raise_for_status()
        found_klasser = response.json()

        if len(found_klasser['results'][0]) == 1:
            klasse_uuid = found_klasser['results'][0][0]
        elif len(found_klasser['results']) > 1:
            raise Exception('Inconsistent Klasser for external employees')
        else:
            _, facet_uuid = self.helper.read_classes_in_facet(facet)
            logger.info('Creating Klasse: {} in {}'.format(navn, facet_uuid))
            klasse_uuid = self._add_klasse_to_lora(bvn, navn, facet_uuid)['uuid']
        return klasse_uuid

    def _find_or_create_unit_and_classes(self):
        """
        Find uuids of the needed unit and classes for the import. If any unit or
        class is missing from MO, it will be created. The function does not
        return anything, but wil leave self.uuids in valid state.
        """
        job_type = self._fc_klasse('jobfunc_ext', 'Ekstern',
                                   'engagement_job_function')
        eng_type = self._fc_klasse('engtype_ext', 'Ekstern', 'engagement_type')
        org_unit_type = self._fc_klasse('unittype_ext', 'Ekstern', 'org_unit_type')

        unit_uuid = self.settings['integrations.ad.import_ou.mo_unit_uuid']
        unit = self.helper.read_ou(uuid=unit_uuid)
        if 'status' in unit:  # Unit does not exist
            payload = payloads.unit_for_externals(unit_uuid, org_unit_type,
                                                  self.org_uuid)
            print(payload)
            logger.debug('Create department payload: {}'.format(payload))
            response = self.helper._mo_post('ou/create', payload)
            response.raise_for_status()
            logger.info('Created unit for external employees')
            logger.debug('Response: {}'.format(response.text))

        uuids = {
            'job_function': job_type,
            'engagement_type': eng_type,
            'unit_uuid': self.settings['integrations.ad.import_ou.mo_unit_uuid']
        }
        self.uuids = uuids

    def _find_external_users_in_mo(self):
        """
        Find all MO users in the unit for external employees.
        """
        mo_users = self.helper.read_organisation_people(
            self.settings['integrations.ad.import_ou.mo_unit_uuid'])
        return mo_users

    def _update_list_of_external_users_in_ad(self):
        """
        Read all users in AD, find the ones that match criterion as external
        users and update self.ad_users to contain these users.
        """
        users = {}

        with open('everything.p', 'rb') as f:
            everything = pickle.load(f)
        # everything = self.ad_reader.read_it_all()
        # with open('everything.p', 'wb') as f:
        #     pickle.dump(everything, f, pickle.HIGHEST_PROTOCOL)

        for user in everything:  # TODO: Name of OU should go in settings.
            if user['DistinguishedName'].find('Ekstern Konsulenter') > 0:
                uuid = user['ObjectGUID']
                users[uuid] = user
        self.users = users

    def _create_user(self, ad_user, edit=True):
        """
        Create or update a user in MO using an AD user as a template.
        The user will share uuid between MO and AD.
        :param ad_user: The ad_object to use as template for MO.
        :param edit: If True only the user object will be updated (to update
        name), if False an engagement and an IT-system will also be added.
        :return: uuid of the the user.
        """
        cpr_raw = ad_user.get(self.settings['integrations.ad.cpr_field'])
        if cpr_raw is None:
            return None
        cpr = cpr_raw.replace('-', '')

        payload = payloads.create_user(cpr, ad_user, self.org_uuid)
        print('Create user: {}'.format(payload))
        logger.info('Create user payload: {}'.format(payload))
        user_uuid = self.helper._mo_post('e/create', payload).json()
        logger.info('Created employee {}'.format(user_uuid))

        if edit is True:
            return user_uuid

        payload = payloads.connect_it_system_to_user(
            ad_user, self.settings['integrations.opus.it_systems.ad']
        )
        logger.debug('AD account payload: {}'.format(payload))
        response = self.helper._mo_post('details/create', payload)
        assert response.status_code == 201
        logger.info('Added AD account info to {}'.format(cpr))

        validity = {
            'from': datetime.datetime.now().strftime('%Y-%m-%d'),
            'to': None
        }

        payload = payloads.create_engagement(
            ad_user=ad_user,
            validity=validity,
            **self.uuids
        )
        response = self.helper._mo_post('details/create', payload)
        logger.info('Create engagement payload: {}'.format(payload))
        assert response.status_code == 201
        logger.info('Added engagement to {}'.format(cpr))

        return user_uuid

    def rename_user(self, mo_uuid):
        """
        This is purely for testing, rename the user to check that the integration
        is able to change it back.
        :param mo_uuid: uuid of the mo user to rename.
        """
        user = self.helper.read_user(user_uuid=mo_uuid)
        print(user)
        payload = {
            'uuid': mo_uuid,
            'givenname': 'Fornavn',
            'surname': 'Efternavnsen',
            'cpr_no': user['cpr_no'],
            'org': {
                'uuid': self.org_uuid
            }
        }
        return_uuid = self.helper._mo_post('e/create', payload).json()
        print(return_uuid)

    def cleanup_removed_users_from_mo(self):
        """
        Remove users in MO if they are no longer found as external users in AD.
        """
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        if self.users is None:
            self._update_list_of_external_users_in_ad()
        mo_users = self._find_external_users_in_mo()
        for key, user in mo_users.items():
            if key not in self.users:
                # This users is in MO but not in AD:
                payload = payloads.terminate_engagement(user['Engagement UUID'],
                                                        yesterday)
                logger.debug('Terminate payload: {}'.format(payload))
                response = self.helper._mo_post('details/terminate', payload)
                logger.debug('Terminate response: {}'.format(response.text))
                assert response.status_code == 200

    def create_or_update_users_in_mo(self):
        """
        Create users in MO that exist in AD but not in MO.
        Update name of users that has changed name in AD.
        """
        if self.uuids is None:
            self._find_or_create_unit_and_classes()
        if self.users is None:
            self._update_list_of_external_users_in_ad()

        for user_uuid, ad_user in self.users.items():
            mo_user = self.helper.read_user(user_uuid=user_uuid)
            if 'status' in mo_user:
                self._create_user(ad_user, edit=False)
            else:
                if (
                        mo_user['givenname'] != ad_user['GivenName'] or
                        mo_user['surname'] != ad_user['Surname']
                ):
                    print()
                    print('Update name')
                    msg = '{}. Given: {}, Sur: {}'
                    print(msg.format('MO', mo_user['givenname'], mo_user['surname']))
                    print(msg.format('AD', ad_user['GivenName'], ad_user['Surname']))
                    self._create_user(ad_user)

    def cli(self):
        """
        Command line interface for the AD to MO user import.
        """

        parser = argparse.ArgumentParser(description='AD->MO user import')
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument('--create-or-update', action='store_true')
        group.add_argument('--cleanup-removed-users', action='store_true')
        group.add_argument('--full-sync', action='store_true')

        args = vars(parser.parse_args())

        if args.get('create_or_update'):
            self.create_or_update_users_in_mo()

        if args.get('cleanup_removed_users'):
            self.cleanup_removed_users_from_mo()

        if args.get('full_sync'):
            self.create_or_update_users_in_mo()
            self.cleanup_removed_users_from_mo()


if __name__ == '__main__':
    ad_import = ADMOImporter()
    ad_import.cli()

    # user_uuid = '1fd162c5-ee26-45e6-a17e-8015869134f0'
    # ad_import.rename_user(user_uuid)
