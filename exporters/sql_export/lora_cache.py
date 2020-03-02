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

        self.dar_cache = {}
        self.mh = MoraHelper(hostname=self.settings['mora.base'], export_ansi=False)
        try:
            self.org_uuid = self.mh.read_organisation()
        except requests.exceptions.RequestException as e:
            logger.error(e)
            print(e)
            exit()

    def _perform_lora_lookup(self, url):
        """
        Exctract a complete set of objects in LoRa.
        :param url: The url that should be used to extract data.
        """
        response = requests.get(self.settings['mox.base'] + url + '&list=1')
        data = response.json()
        results = data['results']
        if results:
            data_list = data['results'][0]
        else:
            data_list = []
        return data_list

    def _cache_lora_facets(self):
        url = '/klassifikation/facet?bvn=%'
        facet_list = self._perform_lora_lookup(url)

        facets = {}
        for facet in facet_list:
            uuid = facet['id']
            reg = facet['registreringer'][0]
            user_key = reg['attributter']['facetegenskaber'][0]['brugervendtnoegle']
            facets[uuid] = {
                'user_key': user_key,
            }
        return facets

    def _cache_lora_classes(self):
        url = '/klassifikation/klasse?bvn=%'
        class_list = self._perform_lora_lookup(url)

        classes = {}
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
        url = '/organisation/itsystem?bvn=%'
        itsystem_list = self._perform_lora_lookup(url)

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
        url = '/organisation/bruger?bvn=%'
        user_list = self._perform_lora_lookup(url)

        users = {}
        for user in user_list:
            uuid = user['id']
            reg = user['registreringer'][0]
            cpr = reg['relationer']['tilknyttedepersoner'][0]['urn'][-10:]
            udv = reg['attributter']['brugerudvidelser'][0]
            fornavn = udv.get('fornavn', '')
            efternavn = udv.get('efternavn', '')
            users[uuid] = {
                'cpr': cpr,
                'fornavn': fornavn,
                'efternavn': efternavn,
                'navn': '{} {}'.format(fornavn, efternavn)
            }
        return users

    def _cache_lora_units(self):
        url = '/organisation/organisationenhed?bvn=%'
        unit_list = self._perform_lora_lookup(url)

        units = {}
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
        url = '/organisation/organisationfunktion?funktionsnavn=Adresse'
        address_list = self._perform_lora_lookup(url)

        total_dar = 0
        no_hit = 0
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
                skip_len = len('urn:dar:')
                dar_uuid = value_raw[skip_len:]
                total_dar += 1

                if self.dar_cache.get(dar_uuid) is None:
                    self.dar_cache[dar_uuid] = {}
                    no_hit += 1
                    for addrtype in ('adresser', 'adgangsadresser'):
                        adr_url = 'https://dawa.aws.dk/{}'.format(addrtype)
                        # 'historik/adresser', 'historik/adgangsadresser'
                        params = {'id': dar_uuid, 'struktur': 'mini'}
                        # Note: Dar accepts up to 10 simultanious connections,
                        # consider grequests.
                        r = requests.get(url=adr_url, params=params)

                        address_data = r.json()
                        r.raise_for_status()
                        if address_data:
                            self.dar_cache[dar_uuid] = address_data[0]
                            break

                if self.dar_cache[dar_uuid]:
                    value = self.dar_cache[dar_uuid].get('betegnelse')
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
        print('Total dar: {}, no-hit: {}'.format(total_dar, no_hit))
        return addresses

    def _cache_lora_engagements(self):
        url = '/organisation/organisationfunktion?funktionsnavn=Engagement'
        engagement_list = self._perform_lora_lookup(url)

        engagements = {}
        for engagement in engagement_list:
            uuid = engagement['id']
            reg = engagement['registreringer'][0]
            user_key = (reg['attributter']['organisationfunktionegenskaber'][0]
                        ['brugervendtnoegle'])
            rel = reg['relationer']
            engagement_type = rel['organisatoriskfunktionstype'][0]['uuid']
            primary_type = rel['primær'][0]['uuid']
            job_function = rel['opgaver'][0]['uuid']
            user_uuid = rel['tilknyttedebrugere'][0]['uuid']
            unit_uuid = rel['tilknyttedeenheder'][0]['uuid']

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
        url = '/organisation/organisationfunktion?funktionsnavn=Tilknytning'
        association_list = self._perform_lora_lookup(url)

        associations = {}
        for association in association_list:
            uuid = association['id']
            reg = association['registreringer'][0]
            user_key = (reg['attributter']['organisationfunktionegenskaber'][0]
                        ['brugervendtnoegle'])
            rel = reg['relationer']
            association_type = rel['organisatoriskfunktionstype'][0]['uuid']
            user_uuid = rel['tilknyttedebrugere'][0]['uuid']
            unit_uuid = rel['tilknyttedeenheder'][0]['uuid']

            associations[uuid] = {
                'uuid': uuid,
                'user': user_uuid,
                'unit': unit_uuid,
                'user_key': user_key,
                'association_type': association_type,
            }
        return associations

    def _cache_lora_roles(self):
        url = '/organisation/organisationfunktion?funktionsnavn=Rolle'
        role_list = self._perform_lora_lookup(url)

        roles = {}
        for role in role_list:
            uuid = role['id']
            reg = role['registreringer'][0]
            role_type = reg['relationer']['organisatoriskfunktionstype'][0]['uuid']
            user_uuid = reg['relationer']['tilknyttedebrugere'][0]['uuid']
            unit_uuid = reg['relationer']['tilknyttedeenheder'][0]['uuid']

            roles[uuid] = {
                'uuid': uuid,
                'user': user_uuid,
                'unit': unit_uuid,
                'role_type': role_type,
            }
        return roles

    def _cache_lora_leaves(self):
        url = '/organisation/organisationfunktion?funktionsnavn=Orlov'
        leave_list = self._perform_lora_lookup(url)

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

    def _cache_lora_it_connections(self):
        url = '/organisation/organisationfunktion?funktionsnavn=IT-system'
        it_connection_list = self._perform_lora_lookup(url)

        it_connections = {}
        for it_connection in it_connection_list:
            uuid = it_connection['id']
            reg = it_connection['registreringer'][0]
            user_key = (reg['attributter']['organisationfunktionegenskaber'][0]
                        ['brugervendtnoegle'])

            itsystem = reg['relationer']['tilknyttedeitsystemer'][0]['uuid']

            if 'tilknyttedeenheder' in reg['relationer']:
                unit_uuid = reg['relationer']['tilknyttedeenheder'][0]['uuid']
                user_uuid = None
            else:
                user_uuid = reg['relationer']['tilknyttedebrugere'][0]['uuid']
                unit_uuid = None

            it_connections[uuid] = {
                'uuid': uuid,
                'user': user_uuid,
                'unit': unit_uuid,
                'username': user_key,
                'itsystem': itsystem
            }
        return it_connections

    def _cache_lora_managers(self):
        url = '/organisation/organisationfunktion?gyldighed=Aktiv&funktionsnavn=Leder'
        manager_list = self._perform_lora_lookup(url)

        managers = {}
        for manager in manager_list:
            print()
            print(manager)
            uuid = manager['id']
            # reg = manager['registreringer'][0]
            rel = manager['registreringer'][0]['relationer']
            user_uuid = rel['tilknyttedebrugere'][0]['uuid']
            unit_uuid = rel['tilknyttedeenheder'][0]['uuid']
            manager_type = rel['organisatoriskfunktionstype'][0]['uuid']
            manager_responsibility = []

            for opgave in rel['opgaver']:
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
        with open('facets.p', 'rb') as f:
            self.facets = pickle.load(f)
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
        with open('roles.p', 'rb') as f:
            self.roles = pickle.load(f)
        with open('itsystems.p', 'rb') as f:
            self.itsystems = pickle.load(f)
        with open('it_connections.p', 'rb') as f:
            self.it_connections = pickle.load(f)

        # t = time.time()
        # msg = 'Kørselstid: {:.1f}s, {} elementer, {:.0f}/s'

        # print('Læs facetter og klasser')
        # self.facets = self._cache_lora_facets()
        # self.classes = self._cache_lora_classes()
        # dt = time.time() - t
        # elements = len(self.classes) + len(self.facets)
        # with open('facets.p', 'wb') as f:
        #     pickle.dump(self.facets, f, pickle.HIGHEST_PROTOCOL)
        # with open('classes.p', 'wb') as f:
        #     pickle.dump(self.classes, f, pickle.HIGHEST_PROTOCOL)
        # print(msg.format(dt, elements, elements/dt))

        # t = time.time()
        # print('Læs brugere')
        # self.users = self._cache_lora_users()
        # dt = time.time() - t
        # with open('users.p', 'wb') as f:
        #     pickle.dump(self.users, f, pickle.HIGHEST_PROTOCOL)
        # print(msg.format(dt, len(self.users), len(self.users)/dt))

        # t = time.time()
        # print('Læs enheder')
        # self.units = self._cache_lora_units()
        # dt = time.time() - t
        # with open('units.p', 'wb') as f:
        #     pickle.dump(self.units, f, pickle.HIGHEST_PROTOCOL)
        # print(msg.format(dt, len(self.units), len(self.units)/dt))

        # t = time.time()
        # print('Læs adresser:')
        # self.addresses = self._cache_lora_address()
        # dt = time.time() - t
        # with open('addresses.p', 'wb') as f:
        #     pickle.dump(self.addresses, f, pickle.HIGHEST_PROTOCOL)
        # print(msg.format(dt, len(self.addresses), len(self.addresses)/dt))

        # t = time.time()
        # print('Læs engagementer')
        # self.engagements = self._cache_lora_engagements()
        # dt = time.time() - t
        # with open('engagements.p', 'wb') as f:
        #     pickle.dump(self.engagements, f, pickle.HIGHEST_PROTOCOL)
        # print(msg.format(dt, len(self.engagements), len(self.engagements)/dt))

        # t = time.time()
        # print('Læs ledere')
        # self.managers = self._cache_lora_managers()
        # dt = time.time() - t
        # with open('managers.p', 'wb') as f:
        #     pickle.dump(self.managers, f, pickle.HIGHEST_PROTOCOL)
        # print(msg.format(dt, len(self.managers), len(self.managers)/dt))

        # t = time.time()
        # print('Læs tilknytninger')
        # self.associations = self._cache_lora_associations()
        # dt = time.time() - t
        # with open('associations.p', 'wb') as f:
        #     pickle.dump(self.associations, f, pickle.HIGHEST_PROTOCOL)
        # print(msg.format(dt, len(self.associations), len(self.associations)/dt))

        # t = time.time()
        # print('Læs orlover')
        # self.leaves = self._cache_lora_leaves()
        # dt = time.time() - t
        # with open('leaves.p', 'wb') as f:
        #     pickle.dump(self.leaves, f, pickle.HIGHEST_PROTOCOL)
        # print(msg.format(dt, len(self.leaves), len(self.leaves)/dt))

        # t = time.time()
        # print('Læs roller')
        # t = time.time()
        # self.roles = self._cache_lora_roles()
        # dt = time.time() - t
        # with open('roles.p', 'wb') as f:
        #     pickle.dump(self.roles, f, pickle.HIGHEST_PROTOCOL)
        # print(msg.format(dt, len(self.roles), len(self.roles)/dt))

        # t = time.time()
        # print('Læs it')
        # self.itsystems = self._cache_lora_itsystems()
        # self.it_connections = self._cache_lora_it_connections()
        # elements = len(self.itsystems) + len(self.it_connections)
        # dt = time.time() - t
        # print(msg.format(dt, elements, elements/dt))
        # with open('itsystems.p', 'wb') as f:
        #     pickle.dump(self.itsystems, f, pickle.HIGHEST_PROTOCOL)
        # with open('it_connections.p', 'wb') as f:
        #     pickle.dump(self.it_connections, f, pickle.HIGHEST_PROTOCOL)


if __name__ == '__main__':
    lc = LoraCache()
    lc.populate_cache()
