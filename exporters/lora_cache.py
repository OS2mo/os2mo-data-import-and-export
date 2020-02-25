import json
import time
import pathlib
import requests

from os2mo_helpers.mora_helpers import MoraHelper


class LoraCache(object):

    def __init__(self):
        cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
        if not cfg_file.is_file():
            raise Exception('No setting file')
        self.settings = json.loads(cfg_file.read_text())

        self.mh = MoraHelper(hostname=self.settings['mora.base'], export_ansi=False)
        try:
            self.org_uuid = self.mh.read_organisation()
        except requests.exceptions.RequestException as e:
            logger.error(e)
            print(e)
            exit()

    def _perform_lora_lookup(self, url, params):
        response = requests.get(self.settings['mox.base'] + url.format(params))
        data = response.json()
        data_list = data['results'][0]
        return data_list

    def _run_buildup(self, uuid_url, data_url):
        """
        Exctract a complete set of objects in LoRa.
        :param uuid_url: The url that should be used to extract uuids from LoRa.
        :param data_url: The url that should be used to extract data.
        """
        data_list = []

        response = requests.get(self.settings['mox.base'] + uuid_url)
        uuids = response.json()

        build_up = '?'
        # TODO! Huske at også fremtidige virkninger måske skal med
        for uuid in uuids['results'][0]:
            build_up += 'uuid=' + uuid + '&'
            if build_up.count('&') < 96:
                continue
            data_list += self._perform_lora_lookup(data_url, build_up[:-1])
            build_up = '?'
            # break
        if not build_up == '?':
            data_list += self._perform_lora_lookup(data_url, build_up[:-1])

        assert len(data_list) == len(uuids['results'][0])
        return data_list

    def _cache_lora_classes(self):
        classes = {}
        uuid_url = '/klassifikation/klasse?bvn=%'
        data_url = '/klassifikation/klasse{}'
        class_list = self._run_buildup(uuid_url, data_url)

        for oio_class in class_list:
            uuid = oio_class['id']
            reg = oio_class['registreringer'][0]
            user_key = reg['attributter']['klasseegenskaber'][0]['brugervendtnoegle']
            title = reg['attributter']['klasseegenskaber'][0]['titel']
            facet = reg['relationer']['facet'][0]['uuid']
            classes[uuid] = {
                'user_key': user_key,
                'title': title,
                'facet': facet
            }
        return classes

    def _cache_lora_itsystems(self):
        uuid_url = '/organisation/itsystem?bvn=%'
        data_url = '/organisation/itsystem{}'
        itsystem_list = self._run_buildup(uuid_url, data_url)

        itsystems = {}
        for itsystem in itsystem_list:
            uuid = itsystem['id']
            reg = itsystem['registreringer'][0]
            user_key = (reg['attributter']['itsystemegenskaber'][0]
                        ['brugervendtnoegle'])
            name = (reg['attributter']['itsystemegenskaber'][0]
                    ['itsystemnavn'])

            itsystems[uuid] = {
                'user_key': user_key,
                'name': name,
            }
        return itsystems

    def _cache_lora_users(self):
        users = {}
        uuid_url = '/organisation/bruger?bvn=%'
        data_url = '/organisation/bruger{}'
        user_list = self._run_buildup(uuid_url, data_url)

        for user in user_list:
            uuid = user['id']
            reg = user['registreringer'][0]
            cpr = reg['relationer']['tilknyttedepersoner'][0]['urn'][-10:]
            fornavn = reg['attributter']['brugerudvidelser'][0]['fornavn']
            efternavn = reg['attributter']['brugerudvidelser'][0]['efternavn']
            users[uuid] = {
                'cpr': cpr,
                'fornavn': fornavn,
                'efternavn': efternavn,
                'navn': '{} {}'.format(fornavn, efternavn)
            }
        return users

    def _cache_lora_units(self):
        units = {}
        uuid_url = '/organisation/organisationenhed?bvn=%'
        data_url = '/organisation/organisationenhed{}'
        unit_list = self._run_buildup(uuid_url, data_url)

        for unit in unit_list:
            uuid = unit['id']
            reg = unit['registreringer'][0]

            egenskaber = reg['attributter']['organisationenhedegenskaber'][0]
            relationer = reg['relationer']

            parent_raw = relationer['overordnet'][0]['uuid']
            if parent_raw == self.org_uuid:
                parent = None
            else:
                parent = parent_raw
            units[uuid] = {
                'user_key': egenskaber['brugervendtnoegle'],
                'name': egenskaber['enhedsnavn'],
                'unit_type': relationer['enhedstype'][0]['uuid'],
                'level': relationer['niveau'][0]['uuid'],
                'parent': parent
            }
        return units

    def _cache_lora_address(self):
        uuid_url = '/organisation/organisationfunktion?funktionsnavn=Adresse'
        data_url = '/organisation/organisationfunktion{}'
        address_list = self._run_buildup(uuid_url, data_url)

        addresses = {}
        for address in address_list:
            uuid = address['id']
            reg = address['registreringer'][0]

            if 'tilknyttedeenheder' in reg['relationer']:
                unit_uuid = reg['relationer']['tilknyttedeenheder'][0]['uuid']
                user_uuid = None
            else:
                user_uuid = reg['relationer']['tilknyttedebrugere'][0]['uuid']
                unit_uuid = None

            dar_uuid = None
            value_raw = reg['relationer']['adresser'][0]['urn']
            address_type = reg['relationer']['adresser'][0]['objekttype']
            if address_type == 'EMAIL':
                scope = 'E-mail'
                skip_len = len('urn:mailto:')
                value = value_raw[skip_len:]
            elif address_type == 'PHONE':
                scope = 'Telefon'
                skip_len = len('urn:magenta.dk:telefon:')
                value = value_raw[skip_len:]
            elif address_type == 'PNUMBER':
                scope = 'P-nummer'
                skip_len = len('urn:dk:cvr:produktionsenhed:')
                value = value_raw[skip_len:]
            elif address_type == 'DAR':
                scope = 'DAR'
                skip_len = len('')
                dar_uuid = value_raw[skip_len:]
                value = ''  # TODO - perform DAR lookup
            else:
                print('Ny type: {}'.format(address_type))
                print(value_raw)
                exit()

            address_type_class = (reg['relationer']['organisatoriskfunktionstype']
                                  [0]['uuid'])

            # Notice: The index-0 assumes that no other tasks are on the address
            synlighed = None
            if 'opgaver' in reg['relationer']:
                if reg['relationer']['opgaver'][0]['objekttype'] == 'synlighed':
                    synlighed = reg['relationer']['opgaver'][0]['uuid']

            addresses[uuid] = {
                'uuid': uuid,
                'user': user_uuid,
                'unit': unit_uuid,
                'value': value,
                'scope': scope,
                'dar_uuid': dar_uuid,
                'adresse_type': address_type_class,
                'visibility': synlighed
            }
        return addresses

    def _cache_lora_engagements(self):
        uuid_url = '/organisation/organisationfunktion?funktionsnavn=Engagement'
        data_url = '/organisation/organisationfunktion{}'
        engagement_list = self._run_buildup(uuid_url, data_url)

        engagements = {}
        for engagement in engagement_list:
            uuid = engagement['id']
            reg = engagement['registreringer'][0]
            user_key = (reg['attributter']['organisationfunktionegenskaber'][0]
                        ['brugervendtnoegle'])
            engagement_type = reg['relationer']['organisatoriskfunktionstype'][0]['uuid']
            primary_type = reg['relationer']['primær'][0]['uuid']
            job_function = reg['relationer']['opgaver'][0]['uuid']
            user_uuid = reg['relationer']['tilknyttedebrugere'][0]['uuid']
            unit_uuid = reg['relationer']['tilknyttedeenheder'][0]['uuid']

            engagements[uuid] = {
                'uuid': uuid,
                'user': user_uuid,
                'unit': unit_uuid,
                'user_key': user_key,
                'engagement_type': engagement_type,
                'primary_type': primary_type,
                'job_function': job_function
            }
        return engagements

    def _cache_lora_associations(self):
        uuid_url = '/organisation/organisationfunktion?funktionsnavn=Tilknytning'
        data_url = '/organisation/organisationfunktion{}'
        association_list = self._run_buildup(uuid_url, data_url)

        associations = {}
        for association in association_list:
            uuid = association['id']
            reg = association['registreringer'][0]
            user_key = (reg['attributter']['organisationfunktionegenskaber'][0]
                        ['brugervendtnoegle'])
            association_type = reg['relationer']['organisatoriskfunktionstype'][0]['uuid']
            user_uuid = reg['relationer']['tilknyttedebrugere'][0]['uuid']
            unit_uuid = reg['relationer']['tilknyttedeenheder'][0]['uuid']

            associations[uuid] = {
                'uuid': uuid,
                'user': user_uuid,
                'unit': unit_uuid,
                'user_key': user_key,
                'association_type': association_type,
            }
        return associations


    def _cache_lora_leaves(self):
        uuid_url = '/organisation/organisationfunktion?funktionsnavn=Orlov'
        data_url = '/organisation/organisationfunktion{}'
        leave_list = self._run_buildup(uuid_url, data_url)

        leaves = {}
        for leave in leave_list:
            uuid = leave['id']
            reg = leave['registreringer'][0]
            user_key = (reg['attributter']['organisationfunktionegenskaber'][0]
                        ['brugervendtnoegle'])
            leave_type = reg['relationer']['organisatoriskfunktionstype'][0]['uuid']
            user_uuid = reg['relationer']['tilknyttedebrugere'][0]['uuid']

            leaves[uuid] = {
                'uuid': uuid,
                'user': user_uuid,
                'user_key': user_key,
                'leave_type': leave_type,
            }
        return leaves


    def _cache_lora_managers(self):
        uuid_url = '/organisation/organisationfunktion?funktionsnavn=Leder'
        data_url = '/organisation/organisationfunktion{}'
        manager_list = self._run_buildup(uuid_url, data_url)

        managers = {}
        for manager in manager_list:
            uuid = manager['id']
            reg = manager['registreringer'][0]
            user_uuid = reg['relationer']['tilknyttedebrugere'][0]['uuid']
            unit_uuid = reg['relationer']['tilknyttedeenheder'][0]['uuid']
            manager_type = reg['relationer']['organisatoriskfunktionstype'][0]['uuid']
            manager_responsibility = []

            for opgave in reg['relationer']['opgaver']:
                if opgave['objekttype'] == 'lederniveau':
                    manager_level = opgave['uuid']
                if opgave['objekttype'] == 'lederansvar':
                    manager_responsibility.append(opgave['uuid'])

            managers[uuid] = {
                'uuid': uuid,
                'user': user_uuid,
                'unit': unit_uuid,
                'manager_type': manager_type,
                'manager_level': manager_level,
                'manager_responsibility': manager_responsibility
            }
        return managers

    def populate_cache(self):
        import pickle
        with open('classes.p', 'rb') as f:
            self.classes = pickle.load(f)
        with open('users.p', 'rb') as f:
            self.users = pickle.load(f)
        with open('units.p', 'rb') as f:
            self.units = pickle.load(f)
        with open('addresses.p', 'rb') as f:
            self.addresses = pickle.load(f)
        with open('engagements.p', 'rb') as f:
            self.engagements = pickle.load(f)
        with open('managers.p', 'rb') as f:
            self.managers = pickle.load(f)
        with open('associations.p', 'rb') as f:
            self.associations = pickle.load(f)
        with open('leaves.p', 'rb') as f:
            self.leaves = pickle.load(f)

        # self.classes = self._cache_lora_classes()
        # self.users = self._cache_lora_users()
        # self.units = self._cache_lora_units()
        # self.addresses = self._cache_lora_address()
        # self.engagements = self._cache_lora_engagements()
        # self.managers = self._cache_lora_managers()
        # self.associations = self._cache_lora_associations()
        # self.leaves = self._cache_lora_leaves()
        self.itsystems = self._cache_lora_itsystems()

        with open('classes.p', 'wb') as f:
            pickle.dump(self.classes, f, pickle.HIGHEST_PROTOCOL)
        with open('users.p', 'wb') as f:
            pickle.dump(self.users, f, pickle.HIGHEST_PROTOCOL)
        with open('units.p', 'wb') as f:
            pickle.dump(self.units, f, pickle.HIGHEST_PROTOCOL)
        with open('addresses.p', 'wb') as f:
            pickle.dump(self.addresses, f, pickle.HIGHEST_PROTOCOL)
        with open('engagements.p', 'wb') as f:
            pickle.dump(self.engagements, f, pickle.HIGHEST_PROTOCOL)
        with open('managers.p', 'wb') as f:
            pickle.dump(self.managers, f, pickle.HIGHEST_PROTOCOL)
        with open('associations.p', 'wb') as f:
            pickle.dump(self.associations, f, pickle.HIGHEST_PROTOCOL)
        with open('leaves.p', 'wb') as f:
            pickle.dump(self.leaves, f, pickle.HIGHEST_PROTOCOL)
        with open('itsystems.p', 'wb') as f:
            pickle.dump(self.itsystems, f, pickle.HIGHEST_PROTOCOL)


if __name__ == '__main__':
    lc = LoraCache()
    lc.populate_cache()
