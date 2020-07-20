import json
import time
import pickle
import urllib
import logging
import pathlib
import datetime
import dateutil
import lora_utils
import requests

from os2mo_helpers.mora_helpers import MoraHelper

logger = logging.getLogger("LoraCache")

DEFAULT_TIMEZONE = dateutil.tz.gettz('Europe/Copenhagen')


class LoraCache(object):

    def __init__(self, resolve_dar=True, full_history=False, skip_past=False):
        msg = 'Start LoRa cache, resolve dar: {}, full_history: {}'
        logger.info(msg.format(resolve_dar, full_history))

        cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
        if not cfg_file.is_file():
            raise Exception('No setting file')
        self.settings = json.loads(cfg_file.read_text())

        self.additional = {
            'relationer': ('tilknyttedeorganisationer', 'tilhoerer')
        }

        if resolve_dar:
            self.dar_cache = {}
        else:
            self.dar_cache = None

        self.full_history = full_history
        self.skip_past = skip_past
        self.org_uuid = self._read_org_uuid()

    def _read_org_uuid(self):
        mh = MoraHelper(hostname=self.settings['mora.base'], export_ansi=False)
        for attempt in range(0, 10):
            try:
                org_uuid = mh.read_organisation()
                return org_uuid
            except (json.decoder.JSONDecodeError, requests.exceptions.RequestException) as e:
                logger.error(e)
                print(e)
                time.sleep(5)
                continue
        # Unable to read org_uuid, must abort
        exit()

    def _get_effects(self, lora_object, relevant):
        effects = lora_utils.get_effects(
            lora_object['registreringer'][0],
            relevant=relevant,
            additional=self.additional
        )
        # Notice, the code below will return the entire validity of an object
        # in the case of non-historic export, this could be handy in some
        # situations, eg ad->mo sync
        # if self.full_history:
        #     effects = lora_utils.get_effects(lora_object['registreringer'][0],
        #                                      relevant=relevant,
        #                                      additional=self.additional)
        # else:
        #     effects = lora_utils.get_effects(lora_object['registreringer'][0],
        #                                      relevant=self.additional,
        #                                      additional=relevant)
        return effects

    def _from_to_from_effect(self, effect):
        """
        Finds to and from date from an effect-row as returned by  iterating over the
        result of _get_effects().
        :param effect: The effect to analyse.
        :return: from_date and to_date. To date can be None, which should be
        interpreted as an infinite validity. In non-historic exports, both values
        can be None, meaning that this row is not the actual-state value.
        """
        dt_from = dateutil.parser.isoparse(str(effect[0]))
        dt_from = dt_from.astimezone(DEFAULT_TIMEZONE)
        from_date = dt_from.date().isoformat()

        if effect[1].replace(tzinfo=None) == datetime.datetime.max:
            to_date = None
        else:
            dt_to = dateutil.parser.isoparse(str(effect[1]))
            dt_to = dt_to.astimezone(DEFAULT_TIMEZONE)
            # MO considers end-dates inclusive, we need to subtract a day
            to_date = (dt_to.date() - datetime.timedelta(days=1)).isoformat()

        now = datetime.datetime.now(DEFAULT_TIMEZONE)
        # If this is an actual state export, we should only return a value if
        # the row is valid today.
        if not self.full_history:
            if to_date is None:
                # In this case, make sure dt_to is bigger than now
                dt_to = now + datetime.timedelta(days=1)
            if not dt_from < now < dt_to:
                from_date = to_date = None

        if self.skip_past:
            if to_date is None:
                # In this case, make sure dt_to is bigger than now
                dt_to = now + datetime.timedelta(days=1)
            if dt_to < now:
                from_date = to_date = None
        return from_date, to_date

    def _perform_lora_lookup(self, url, params, skip_history=False):
        """
        Exctract a complete set of objects in LoRa.
        :param url: The url that should be used to extract data.
        :param skip_history: Force a validity of today, even if self.full_history
        is true.
        """
        t = time.time()
        logger.debug('Start reading {}, params: {}, at t={}'.format(url, params, t))
        results_pr_request = 5000
        params['list'] = 1
        params['maximalantalresultater'] = results_pr_request
        params['foersteresultat'] = 0

        # Default, this can be overwritten in the lines below
        now = datetime.datetime.today()
        params['virkningFra'] = now.strftime('%Y-%m-%d') + " 00:00:00"
        params['virkningTil'] = now.strftime('%Y-%m-%d') + " 00:00:01"
        if self.full_history and not skip_history:
            params['virkningTil'] = 'infinity'
            if not self.skip_past:
                params['virkningFra'] = '-infinity'

        complete_data = []

        done = False
        while not done:
            response = requests.get(self.settings['mox.base'] + url, params=params)
            data = response.json()
            results = data['results']
            if results:
                data_list = data['results'][0]
            else:
                data_list = []
            complete_data = complete_data + data_list
            if len(data_list) == 0:
                done = True
            else:
                params['foersteresultat'] += results_pr_request
                logger.debug('Mellemtid, {} læsninger: {}s'.format(
                    params['foersteresultat'], time.time() - t))
        logger.debug('LoRa læsning færdig. {} elementer, {}s'.format(
            len(complete_data), time.time() - t))
        return complete_data

    def _cache_lora_facets(self):
        # Facets are eternal i MO and does not need a historic dump
        params = {'bvn': '%'}
        url = '/klassifikation/facet'
        facet_list = self._perform_lora_lookup(url, params, skip_history=True)

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
        # MO itself will not read historic information on classes,
        # currently we replicate this behaviour here.
        params = {'bvn': '%'}
        url = '/klassifikation/klasse'
        class_list = self._perform_lora_lookup(url, params, skip_history=True)

        classes = {}
        for oio_class in class_list:
            uuid = oio_class['id']
            reg = oio_class['registreringer'][0]
            user_key = reg['attributter']['klasseegenskaber'][0]['brugervendtnoegle']
            scope = reg['attributter']['klasseegenskaber'][0].get('omfang')
            title = reg['attributter']['klasseegenskaber'][0]['titel']
            facet = reg['relationer']['facet'][0]['uuid']
            classes[uuid] = {
                'user_key': user_key,
                'title': title,
                'scope': scope,
                'facet': facet
            }
        return classes

    def _cache_lora_itsystems(self):
        # IT-systems are eternal i MO and does not need a historic dump
        params = {'bvn': '%'}
        url = '/organisation/itsystem'
        itsystem_list = self._perform_lora_lookup(url, params, skip_history=True)

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
        # MO assigns no validity to users, no historic export here.
        params = {'bvn': '%'}
        url = '/organisation/bruger'
        user_list = self._perform_lora_lookup(url, params, skip_history=True)

        users = {}
        for user in user_list:
            uuid = user['id']
            reg = user['registreringer'][0]
            cpr = reg['relationer']['tilknyttedepersoner'][0]['urn'][-10:]
            udv = reg['attributter']['brugerudvidelser'][0]
            fornavn = udv.get('fornavn', '')
            efternavn = udv.get('efternavn', '')
            users[uuid] = {
                'uuid': uuid,
                'cpr': cpr,
                'fornavn': fornavn,
                'efternavn': efternavn,
                'navn': '{} {}'.format(fornavn, efternavn)
            }
        return users

    def _cache_lora_units(self):
        params = {'bvn': '%'}
        url = '/organisation/organisationenhed'
        relevant = {
            'relationer': ('overordnet', 'enhedstype', 'niveau'),
            'attributter': ('organisationenhedegenskaber',)
        }
        unit_list = self._perform_lora_lookup(url, params)

        units = {}
        for unit in unit_list:
            uuid = unit['id']
            units[uuid] = []

            effects = self._get_effects(unit, relevant)
            for effect in effects:
                from_date, to_date = self._from_to_from_effect(effect)
                if from_date is None and to_date is None:
                    continue
                relationer = effect[2]['relationer']

                orgegenskaber = (effect[2]['attributter']
                                 ['organisationenhedegenskaber'])
                if len(orgegenskaber) == 0:
                    continue
                egenskaber = orgegenskaber[0]
                parent_raw = relationer['overordnet'][0]['uuid']
                if parent_raw == self.org_uuid:
                    parent = None
                else:
                    parent = parent_raw

                if 'niveau' in relationer:
                    level = relationer['niveau'][0]['uuid']
                else:
                    level = None
                units[uuid].append(
                    {
                        'uuid': uuid,
                        'user_key': egenskaber['brugervendtnoegle'],
                        'name': egenskaber['enhedsnavn'],
                        'unit_type': relationer['enhedstype'][0]['uuid'],
                        'level': level,
                        'parent': parent,
                        'from_date': from_date,
                        'to_date': to_date
                    }
                )
        return units

    def _cache_lora_address(self):
        params = {'gyldighed': 'Aktiv', 'funktionsnavn': 'Adresse'}

        url = '/organisation/organisationfunktion'
        relevant = {
            'relationer': ('tilknyttedeenheder', 'tilknyttedebrugere',
                           'adresser', 'organisatoriskfunktionstype', 'opgaver'),
            'attributter': ('organisationfunktionegenskaber',)
        }
        address_list = self._perform_lora_lookup(url, params)

        total_dar = 0
        no_hit = 0
        addresses = {}
        logger.info('Performing DAR lookup, this might take a while...')
        for address in address_list:
            uuid = address['id']
            addresses[uuid] = []

            effects = self._get_effects(address, relevant)
            for effect in effects:
                from_date, to_date = self._from_to_from_effect(effect)
                if from_date is None and to_date is None:
                    continue
                relationer = effect[2]['relationer']

                if 'tilknyttedeenheder' in relationer:
                    unit_uuid = relationer['tilknyttedeenheder'][0]['uuid']
                    user_uuid = None
                else:
                    user_uuid = relationer['tilknyttedebrugere'][0]['uuid']
                    unit_uuid = None

                dar_uuid = None
                value_raw = relationer['adresser'][0]['urn']
                address_type = relationer['adresser'][0]['objekttype']
                if address_type == 'EMAIL':
                    scope = 'E-mail'
                    skip_len = len('urn:mailto:')
                    value = value_raw[skip_len:]
                elif address_type == 'WWW':
                    scope = 'Url'
                    skip_len = len('urn:magenta.dk:www:')
                    value = value_raw[skip_len:]
                elif address_type == 'PHONE':
                    scope = 'Telefon'
                    skip_len = len('urn:magenta.dk:telefon:')
                    value = value_raw[skip_len:]
                elif address_type == 'PNUMBER':
                    scope = 'P-nummer'
                    skip_len = len('urn:dk:cvr:produktionsenhed:')
                    value = value_raw[skip_len:]
                elif address_type == 'EAN':
                    scope = 'EAN'
                    skip_len = len('urn:magenta.dk:ean:')
                    value = value_raw[skip_len:]
                elif address_type == 'TEXT':
                    scope = 'Text'
                    skip_len = len('urn:text:')
                    value = urllib.parse.unquote(value_raw[skip_len:])
                elif address_type == 'DAR':
                    # print('Total dar: {}, no-hit: {}'.format(total_dar, no_hit))

                    scope = 'DAR'
                    skip_len = len('urn:dar:')
                    dar_uuid = value_raw[skip_len:]
                    total_dar += 1

                    if self.dar_cache is None:
                        value = None
                    else:
                        if self.dar_cache.get(dar_uuid) is None:
                            self.dar_cache[dar_uuid] = {}
                            no_hit += 1
                            for addrtype in ('adresser', 'adgangsadresser'):
                                logger.debug('Looking up dar: {}'.format(dar_uuid))
                                adr_url = 'https://dawa.aws.dk/{}'.format(addrtype)
                                # 'historik/adresser', 'historik/adgangsadresser'
                                params = {'id': dar_uuid, 'struktur': 'mini'}
                                # Note: Dar accepts up to 10 simultanious
                                # connections, consider grequests.
                                r = requests.get(url=adr_url, params=params)
                                address_data = r.json()
                                r.raise_for_status()
                                if address_data:
                                    self.dar_cache[dar_uuid] = address_data[0]
                                    break
                                self.dar_cache[dar_uuid] = {'betegelse': 'skip dar'}
                        value = self.dar_cache[dar_uuid].get('betegnelse')
                else:
                    print('Ny type: {}'.format(address_type))
                    msg = 'Unknown addresse type: {}, value: {}'
                    logger.error(msg.format(address_type, value_raw))
                    raise('Unknown address type: {}'.format(address_type))

                address_type_class = (relationer['organisatoriskfunktionstype']
                                      [0]['uuid'])

                synlighed = None
                if relationer.get('opgaver'):
                    if relationer['opgaver'][0]['objekttype'] == 'synlighed':
                        synlighed = relationer['opgaver'][0]['uuid']

                addresses[uuid].append(
                    {
                        'uuid': uuid,
                        'user': user_uuid,
                        'unit': unit_uuid,
                        'value': value,
                        'scope': scope,
                        'dar_uuid': dar_uuid,
                        'adresse_type': address_type_class,
                        'visibility': synlighed,
                        'from_date': from_date,
                        'to_date': to_date
                    }
                )
        logger.info('Total dar: {}, no-hit: {}'.format(total_dar, no_hit))
        return addresses

    def _cache_lora_engagements(self):
        params = {'gyldighed': 'Aktiv', 'funktionsnavn': 'Engagement'}
        relevant = {
            'relationer': ('opgaver', 'tilknyttedeenheder', 'tilknyttedebrugere',
                           'organisatoriskfunktionstype', 'primær'),
            'attributter': ('organisationfunktionegenskaber',
                            'organisationfunktionudvidelser'),
            'tilstande': ('organisationfunktiongyldighed',)
        }
        url = '/organisation/organisationfunktion'
        engagements = {}
        engagement_list = self._perform_lora_lookup(url, params)
        for engagement in engagement_list:
            uuid = engagement['id']
            engagements[uuid] = []

            effects = self._get_effects(engagement, relevant)
            for effect in effects:
                from_date, to_date = self._from_to_from_effect(effect)
                if from_date is None and to_date is None:
                    continue

                # Todo, this should be consistently implemented for all objects
                gyldighed = effect[2]['tilstande']['organisationfunktiongyldighed']
                if not gyldighed:
                    continue
                if not gyldighed[0]['gyldighed'] == 'Aktiv':
                    continue

                attr = effect[2]['attributter']
                rel = effect[2]['relationer']

                if not rel['organisatoriskfunktionstype']:
                    msg = 'Missing in organisatoriskfunktionstype in {}'
                    logger.error(msg.format(engagement))
                    continue

                user_key = (attr['organisationfunktionegenskaber'][0]
                            ['brugervendtnoegle'])

                engagement_type = rel['organisatoriskfunktionstype'][0]['uuid']

                primary_type = None
                primær = rel.get('primær')
                if primær:
                    primary_type = primær[0]['uuid']

                job_function = rel['opgaver'][0]['uuid']

                user_uuid = rel['tilknyttedebrugere'][0]['uuid']
                unit_uuid = rel['tilknyttedeenheder'][0]['uuid']

                udvidelser = {}
                udv_raw = attr.get('organisationfunktionudvidelser')
                if isinstance(udv_raw, list):
                    if len(udv_raw) == 1:
                        udvidelser = udv_raw[0]
                    if len(udv_raw) > 1:
                        msg = 'Ugyldig organisationfunktionudvidelser: {}'
                        raise Exception(msg.format(udv_raw))
                fraction = udvidelser.get('fraktion')
                extensions = {
                    'udvidelse_1': udvidelser.get('udvidelse_1'),
                    'udvidelse_2': udvidelser.get('udvidelse_2'),
                    'udvidelse_3': udvidelser.get('udvidelse_3'),
                    'udvidelse_4': udvidelser.get('udvidelse_4'),
                    'udvidelse_5': udvidelser.get('udvidelse_5'),
                    'udvidelse_6': udvidelser.get('udvidelse_6'),
                    'udvidelse_7': udvidelser.get('udvidelse_7'),
                    'udvidelse_8': udvidelser.get('udvidelse_8'),
                    'udvidelse_9': udvidelser.get('udvidelse_9'),
                    'udvidelse_10': udvidelser.get('udvidelse_10')
                }

                engagements[uuid].append(
                    {
                        'uuid': uuid,
                        'user': user_uuid,
                        'unit': unit_uuid,
                        'fraction': fraction,
                        'user_key': user_key,
                        'engagement_type': engagement_type,
                        'primary_type': primary_type,
                        'job_function': job_function,
                        'extensions': extensions,
                        'from_date': from_date,
                        'to_date': to_date
                    }
                )
        return engagements

    def _cache_lora_associations(self):
        params = {'gyldighed': 'Aktiv', 'funktionsnavn': 'Tilknytning'}
        relevant = {
            'relationer': ('tilknyttedeenheder', 'tilknyttedebrugere',
                           'organisatoriskfunktionstype'),
            'attributter': ('organisationfunktionegenskaber',)
        }
        url = '/organisation/organisationfunktion'
        associations = {}
        association_list = self._perform_lora_lookup(url, params)
        for association in association_list:
            uuid = association['id']
            associations[uuid] = []

            effects = self._get_effects(association, relevant)
            for effect in effects:
                from_date, to_date = self._from_to_from_effect(effect)
                if from_date is None and to_date is None:
                    continue

                attr = effect[2]['attributter']
                rel = effect[2]['relationer']

                if rel['tilknyttedeenheder']:
                    unit_uuid = rel['tilknyttedeenheder'][0]['uuid']
                else:
                    unit_uuid = None
                    logger.error('Error: Unable to find unit in {}'.format(uuid))

                user_key = (attr['organisationfunktionegenskaber'][0]
                            ['brugervendtnoegle'])
                association_type = rel['organisatoriskfunktionstype'][0]['uuid']
                user_uuid = rel['tilknyttedebrugere'][0]['uuid']

                associations[uuid].append(
                     {
                         'uuid': uuid,
                         'user': user_uuid,
                         'unit': unit_uuid,
                         'user_key': user_key,
                         'association_type': association_type,
                         'from_date': from_date,
                         'to_date': to_date
                     }
                )
        return associations

    def _cache_lora_roles(self):
        params = {'gyldighed': 'Aktiv', 'funktionsnavn': 'Rolle'}
        relevant = {
            'relationer': ('tilknyttedeenheder', 'tilknyttedebrugere',
                           'organisatoriskfunktionstype')
        }
        url = '/organisation/organisationfunktion'
        roles = {}
        role_list = self._perform_lora_lookup(url, params)
        for role in role_list:
            uuid = role['id']
            roles[uuid] = []

            effects = self._get_effects(role, relevant)
            for effect in effects:
                from_date, to_date = self._from_to_from_effect(effect)
                if from_date is None and to_date is None:
                    continue
                rel = effect[2]['relationer']
                role_type = rel['organisatoriskfunktionstype'][0]['uuid']
                user_uuid = rel['tilknyttedebrugere'][0]['uuid']
                unit_uuid = rel['tilknyttedeenheder'][0]['uuid']

                roles[uuid].append(
                    {
                        'uuid': uuid,
                        'user': user_uuid,
                        'unit': unit_uuid,
                        'role_type': role_type,
                        'from_date': from_date,
                        'to_date': to_date
                    }
                )
        return roles

    def _cache_lora_leaves(self):
        params = {'gyldighed': 'Aktiv', 'funktionsnavn': 'Orlov'}
        relevant = {
            'relationer': ('tilknyttedebrugere', 'organisatoriskfunktionstype'),
            'attributter': ('organisationfunktionegenskaber',)
        }
        url = '/organisation/organisationfunktion'
        leaves = {}
        leave_list = self._perform_lora_lookup(url, params)
        for leave in leave_list:
            uuid = leave['id']
            leaves[uuid] = []
            effects = self._get_effects(leave, relevant)
            for effect in effects:
                from_date, to_date = self._from_to_from_effect(effect)
                if from_date is None and to_date is None:
                    continue
                attr = effect[2]['attributter']
                rel = effect[2]['relationer']
                user_key = (attr['organisationfunktionegenskaber'][0]
                            ['brugervendtnoegle'])
                leave_type = rel['organisatoriskfunktionstype'][0]['uuid']
                user_uuid = rel['tilknyttedebrugere'][0]['uuid']

                leaves[uuid].append(
                    {
                        'uuid': uuid,
                        'user': user_uuid,
                        'user_key': user_key,
                        'leave_type': leave_type,
                        'from_date': from_date,
                        'to_date': to_date
                    }
                )
        return leaves

    def _cache_lora_it_connections(self):
        params = {'gyldighed': 'Aktiv', 'funktionsnavn': 'IT-system'}
        url = '/organisation/organisationfunktion'
        it_connection_list = self._perform_lora_lookup(url, params)

        it_connections = {}
        for it_connection in it_connection_list:
            uuid = it_connection['id']
            it_connections[uuid] = []

            relevant = {
                'relationer': ('tilknyttedeenheder', 'tilknyttedebrugere',
                               'tilknyttedeitsystemer'),
                'attributter': ('organisationfunktionegenskaber',)
            }

            effects = self._get_effects(it_connection, relevant)
            for effect in effects:
                from_date, to_date = self._from_to_from_effect(effect)
                if from_date is None and to_date is None:
                    continue
                user_key = (
                    effect[2]['attributter']['organisationfunktionegenskaber']
                    [0]['brugervendtnoegle']
                )

                rel = effect[2]['relationer']
                itsystem = rel['tilknyttedeitsystemer'][0]['uuid']

                if 'tilknyttedeenheder' in rel:
                    unit_uuid = rel['tilknyttedeenheder'][0]['uuid']
                    user_uuid = None
                else:
                    user_uuid = rel['tilknyttedebrugere'][0]['uuid']
                    unit_uuid = None

                it_connections[uuid].append(
                    {
                        'uuid': uuid,
                        'user': user_uuid,
                        'unit': unit_uuid,
                        'username': user_key,
                        'itsystem': itsystem,
                        'from_date': from_date,
                        'to_date': to_date
                    }
                )
        return it_connections

    def _cache_lora_kles(self):
        params = {'gyldighed': 'Aktiv', 'funktionsnavn': 'KLE'}
        url = '/organisation/organisationfunktion'
        kle_list = self._perform_lora_lookup(url, params)
        kles = {}
        for kle in kle_list:
            uuid = kle['id']
            kles[uuid] = []

            relevant = {
                'relationer': ('opgaver', 'tilknyttedeenheder',
                               'organisatoriskfunktionstype'),
                'attributter': ('organisationfunktionegenskaber',)
            }

            effects = self._get_effects(kle, relevant)
            for effect in effects:
                from_date, to_date = self._from_to_from_effect(effect)
                if from_date is None and to_date is None:
                    continue

                user_key = (
                    effect[2]['attributter']['organisationfunktionegenskaber']
                    [0]['brugervendtnoegle']
                )

                rel = effect[2]['relationer']
                unit_uuid = rel['tilknyttedeenheder'][0]['uuid']
                kle_number = rel['organisatoriskfunktionstype'][0]['uuid']
                kle_aspect = rel['opgaver'][0]['uuid']

                kles[uuid].append(
                    {
                        'uuid': uuid,
                        'unit': unit_uuid,
                        'kle_number': kle_number,
                        'kle_aspect': kle_aspect,
                        'user_key': user_key,
                        'from_date': from_date,
                        'to_date': to_date
                    }
                )
        return kles

    def _cache_lora_managers(self):
        params = {'gyldighed': 'Aktiv', 'funktionsnavn': 'Leder'}
        url = '/organisation/organisationfunktion'
        manager_list = self._perform_lora_lookup(url, params)

        managers = {}
        for manager in manager_list:
            uuid = manager['id']
            managers[uuid] = []
            relevant = {
                'relationer': ('opgaver', 'tilknyttedeenheder', 'tilknyttedebrugere',
                               'organisatoriskfunktionstype')
            }

            if self.full_history:
                effects = lora_utils.get_effects(manager['registreringer'][0],
                                                 relevant=relevant,
                                                 additional=self.additional)
            else:
                effects = lora_utils.get_effects(manager['registreringer'][0],
                                                 relevant=self.additional,
                                                 additional=relevant)

            for effect in effects:
                from_date, to_date = self._from_to_from_effect(effect)
                rel = effect[2]['relationer']
                user_uuid = rel['tilknyttedebrugere'][0]['uuid']
                unit_uuid = rel['tilknyttedeenheder'][0]['uuid']
                manager_type = rel['organisatoriskfunktionstype'][0]['uuid']
                manager_responsibility = []

                for opgave in rel['opgaver']:
                    if opgave['objekttype'] == 'lederniveau':
                        manager_level = opgave['uuid']
                    if opgave['objekttype'] == 'lederansvar':
                        manager_responsibility.append(opgave['uuid'])

                managers[uuid].append(
                    {
                        'uuid': uuid,
                        'user': user_uuid,
                        'unit': unit_uuid,
                        'manager_type': manager_type,
                        'manager_level': manager_level,
                        'manager_responsibility': manager_responsibility,
                        'from_date': from_date,
                        'to_date': to_date
                    }
                )
        return managers

    def calculate_primary_engagements(self):
        if self.full_history:
            msg = """
            Calculation of primary engagements is currently not implemented for
            full historic export.
            """
            print(msg)
            return

        user_primary = {}
        for uuid, eng_validities in self.engagements.items():
            assert(len(eng_validities)) == 1
            eng = eng_validities[0]

            primary_type = self.classes.get(eng['primary_type'])
            if primary_type is None:
                msg = 'Primary information missing in engagement {}'
                logger.debug(msg.format(uuid))
                continue
            primary_scope = int(primary_type['scope'])
            if eng['user'] in user_primary:
                if user_primary[eng['user']][0] < primary_scope:
                    user_primary[eng['user']] = [primary_scope, uuid, None]
            else:
                user_primary[eng['user']] = [primary_scope, uuid, None]

        for uuid, eng_validities in self.engagements.items():
            eng = eng_validities[0]
            primary_for_user = user_primary.get(eng['user'], [None, None, None])
            if primary_for_user[1] == uuid:
                logger.debug('Primary for {} is {}'.format(eng['user'], uuid))
                self.engagements[uuid][0]['primary_boolean'] = True
            else:
                logger.debug('{} is not primary {}'.format(uuid, eng['user']))
                self.engagements[uuid][0]['primary_boolean'] = False

    def calculate_derived_unit_data(self):
        if self.full_history:
            msg = """
            Calculation of derived unit data is currently not implemented for
            full historic export.
            """
            print(msg)
            return

        responsibility_class = self.settings[
            'exporters.actual_state.manager_responsibility_class']
        for unit, unit_validities in self.units.items():
            assert(len(unit_validities)) == 1
            unit_info = unit_validities[0]
            manager_uuid = None
            acting_manager_uuid = None

            # Find a direct manager, if possible
            for manager, manager_validities in self.managers.items():
                assert(len(manager_validities)) == 1
                manager_info = manager_validities[0]

                if manager_info['unit'] == unit:
                    for resp in manager_info['manager_responsibility']:
                        if resp == responsibility_class:
                            manager_uuid = manager
                            acting_manager_uuid = manager

            location = ''
            current_unit = unit_info
            while current_unit:
                location = current_unit['name'] + "\\" + location
                current_parent = current_unit.get('parent')
                if current_parent is not None and current_parent in self.units:
                    current_unit = self.units[current_parent][0]
                else:
                    current_unit = None

                # Find the acting manager.
                if acting_manager_uuid is None:
                    for manager, manager_validities in self.managers.items():
                        manager_info = manager_validities[0]
                        if manager_info['unit'] == current_parent:
                            for resp in manager_info['manager_responsibility']:
                                if resp == responsibility_class:
                                    acting_manager_uuid = manager
            location = location[:-1]

            self.units[unit][0]['location'] = location
            self.units[unit][0]['manager_uuid'] = manager_uuid
            self.units[unit][0]['acting_manager_uuid'] = acting_manager_uuid

    def populate_cache(self, dry_run=False, skip_associations=False):
        """
        Perform the actual data import.
        :param skip_associations: If associations are not needed, they can be
        skipped for increased performance.
        :param dry_run: For testing purposes it is possible to read from cache.
        """
        if self.full_history:
            facets_file = 'tmp/facets_historic.p'
            classes_file = 'tmp/classes_historic.p'
            users_file = 'tmp/users_historic.p'
            units_file = 'tmp/units_historic.p'
            addresses_file = 'tmp/addresses_historic.p'
            engagements_file = 'tmp/engagements_historic.p'
            managers_file = 'tmp/managers_historic.p'
            associations_file = 'tmp/associations_historic.p'
            leaves_file = 'tmp/leaves_historic.p'
            roles_file = 'tmp/roles_historic.p'
            itsystems_file = 'tmp/itsystems_historic.p'
            it_connections_file = 'tmp/it_connections_historic.p'
            kles_file = 'tmp/kles_historic.p'
        else:
            facets_file = 'tmp/facets.p'
            classes_file = 'tmp/classes.p'
            users_file = 'tmp/users.p'
            units_file = 'tmp/units.p'
            addresses_file = 'tmp/addresses.p'
            engagements_file = 'tmp/engagements.p'
            managers_file = 'tmp/managers.p'
            associations_file = 'tmp/associations.p'
            leaves_file = 'tmp/leaves.p'
            roles_file = 'tmp/roles.p'
            itsystems_file = 'tmp/itsystems.p'
            it_connections_file = 'tmp/it_connections.p'
            kles_file = 'tmp/kles.p'

        if dry_run:
            logger.info('LoRa cache dry run - no actual read')
            with open(facets_file, 'rb') as f:
                self.facets = pickle.load(f)
            with open(classes_file, 'rb') as f:
                self.classes = pickle.load(f)
            with open(users_file, 'rb') as f:
                self.users = pickle.load(f)
            with open(units_file, 'rb') as f:
                self.units = pickle.load(f)
            with open(addresses_file, 'rb') as f:
                self.addresses = pickle.load(f)
            with open(engagements_file, 'rb') as f:
                self.engagements = pickle.load(f)
            with open(managers_file, 'rb') as f:
                self.managers = pickle.load(f)

            if not skip_associations:
                with open(associations_file, 'rb') as f:
                    self.associations = pickle.load(f)

            with open(leaves_file, 'rb') as f:
                self.leaves = pickle.load(f)
            with open(roles_file, 'rb') as f:
                self.roles = pickle.load(f)
            with open(itsystems_file, 'rb') as f:
                self.itsystems = pickle.load(f)
            with open(it_connections_file, 'rb') as f:
                self.it_connections = pickle.load(f)
            with open(kles_file, 'rb') as f:
                self.kles = pickle.load(f)
            return

        t = time.time()
        msg = 'Kørselstid: {:.1f}s, {} elementer, {:.0f}/s'

        # Here we should activate read-only mode
        logger.info('Læs facetter og klasser')
        self.facets = self._cache_lora_facets()
        self.classes = self._cache_lora_classes()
        dt = time.time() - t
        elements = len(self.classes) + len(self.facets)
        with open(facets_file, 'wb') as f:
            pickle.dump(self.facets, f, pickle.HIGHEST_PROTOCOL)
        with open(classes_file, 'wb') as f:
            pickle.dump(self.classes, f, pickle.HIGHEST_PROTOCOL)
        logger.info(msg.format(dt, elements, elements/dt))

        t = time.time()
        logger.info('Læs brugere')
        self.users = self._cache_lora_users()
        dt = time.time() - t
        with open(users_file, 'wb') as f:
            pickle.dump(self.users, f, pickle.HIGHEST_PROTOCOL)
        logger.info(msg.format(dt, len(self.users), len(self.users)/dt))

        t = time.time()
        logger.info('Læs enheder')
        self.units = self._cache_lora_units()
        dt = time.time() - t
        with open(units_file, 'wb') as f:
            pickle.dump(self.units, f, pickle.HIGHEST_PROTOCOL)
        logger.info(msg.format(dt, len(self.units), len(self.units)/dt))

        t = time.time()
        logger.info('Læs adresser:')
        self.addresses = self._cache_lora_address()
        dt = time.time() - t
        with open(addresses_file, 'wb') as f:
            pickle.dump(self.addresses, f, pickle.HIGHEST_PROTOCOL)
        logger.info(msg.format(dt, len(self.addresses), len(self.addresses)/dt))

        t = time.time()
        logger.info('Læs engagementer')
        self.engagements = self._cache_lora_engagements()
        dt = time.time() - t
        with open(engagements_file, 'wb') as f:
            pickle.dump(self.engagements, f, pickle.HIGHEST_PROTOCOL)
        logger.info(msg.format(dt, len(self.engagements), len(self.engagements)/dt))

        t = time.time()
        logger.info('Læs ledere')
        self.managers = self._cache_lora_managers()
        dt = time.time() - t
        with open(managers_file, 'wb') as f:
            pickle.dump(self.managers, f, pickle.HIGHEST_PROTOCOL)
        logger.info(msg.format(dt, len(self.managers), len(self.managers)/dt))

        if not skip_associations:
            t = time.time()
            logger.info('Læs tilknytninger')
            self.associations = self._cache_lora_associations()
            dt = time.time() - t
            with open(associations_file, 'wb') as f:
                pickle.dump(self.associations, f, pickle.HIGHEST_PROTOCOL)
                logger.info(msg.format(dt, len(self.associations),
                                       len(self.associations)/dt))

        t = time.time()
        logger.info('Læs orlover')
        self.leaves = self._cache_lora_leaves()
        dt = time.time() - t
        with open(leaves_file, 'wb') as f:
            pickle.dump(self.leaves, f, pickle.HIGHEST_PROTOCOL)
        logger.info(msg.format(dt, len(self.leaves), len(self.leaves)/dt))

        t = time.time()
        logger.info('Læs roller')
        t = time.time()
        self.roles = self._cache_lora_roles()
        dt = time.time() - t
        with open(roles_file, 'wb') as f:
            pickle.dump(self.roles, f, pickle.HIGHEST_PROTOCOL)
        logger.info(msg.format(dt, len(self.roles), len(self.roles)/dt))

        t = time.time()
        logger.info('Læs it')
        self.itsystems = self._cache_lora_itsystems()
        self.it_connections = self._cache_lora_it_connections()
        elements = len(self.itsystems) + len(self.it_connections)
        dt = time.time() - t
        with open(itsystems_file, 'wb') as f:
            pickle.dump(self.itsystems, f, pickle.HIGHEST_PROTOCOL)
        with open(it_connections_file, 'wb') as f:
            pickle.dump(self.it_connections, f, pickle.HIGHEST_PROTOCOL)
        logger.info(msg.format(dt, elements, elements/dt))

        t = time.time()
        logger.info('Læs kles')
        self.kles = self._cache_lora_kles()
        dt = time.time() - t
        with open(kles_file, 'wb') as f:
            pickle.dump(self.kles, f, pickle.HIGHEST_PROTOCOL)
        logger.info(msg.format(dt, len(self.kles), len(self.kles)/dt))
        # Here we should de-activate read-only mode


if __name__ == '__main__':
    LOG_LEVEL = logging.DEBUG
    LOG_FILE = 'lora_cache.log'

    for name in logging.root.manager.loggerDict:
        if name in ('LoraCache'):
            logging.getLogger(name).setLevel(LOG_LEVEL)
        else:
            logging.getLogger(name).setLevel(logging.ERROR)

    logging.basicConfig(
        format='%(levelname)s %(asctime)s %(name)s %(message)s',
        level=LOG_LEVEL,
        filename=LOG_FILE
    )

    lc = LoraCache(full_history=True, skip_past=True, resolve_dar=False)
    lc.populate_cache(dry_run=False)

    logger.info('Now calcualate derived data')
    lc.calculate_derived_unit_data()
    lc.calculate_primary_engagements()
