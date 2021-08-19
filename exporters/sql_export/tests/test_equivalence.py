import pytest
import json
from tqdm import tqdm
from exporters.sql_export.lora_cache import LoraCache


skip = pytest.mark.skip
# Uncomment the line below to actually run all these equivalence tests.
# NOTE: They will run against the configured live MO / LoRa instances.
# skip = lambda func: func


def _old_cache_lora_facets(self):
    # Facets are eternal i MO and does not need a historic dump
    params = {'bvn': '%'}
    url = '/klassifikation/facet'
    facet_list = self._perform_lora_lookup(url, params, skip_history=True, unit="facet")

    facets = {}
    for facet in tqdm(facet_list, desc="Processing facet", unit="facet"):
        uuid = facet['id']
        reg = facet['registreringer'][0]
        user_key = reg['attributter']['facetegenskaber'][0]['brugervendtnoegle']
        facets[uuid] = {
            'user_key': user_key,
        }
    return facets


@skip
@pytest.mark.parametrize("full_history", [True, False])
@pytest.mark.parametrize("skip_past", [True, False])
def test_facet_equivalence(full_history, skip_past):
    lc = LoraCache(full_history=full_history, skip_past=skip_past, resolve_dar=False)
    old_facets = _old_cache_lora_facets(lc)
    new_facets = lc._cache_lora_facets()
    assert new_facets == old_facets


def _old_cache_lora_classes(self):
    # MO itself will not read historic information on classes,
    # currently we replicate this behaviour here.
    params = {'bvn': '%'}
    url = '/klassifikation/klasse'
    class_list = self._perform_lora_lookup(url, params, skip_history=True, unit="class")

    classes = {}
    for oio_class in tqdm(class_list, desc="Processing class", unit="class"):
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


@skip
@pytest.mark.parametrize("full_history", [True, False])
@pytest.mark.parametrize("skip_past", [True, False])
def test_class_equivalence(full_history, skip_past):
    lc = LoraCache(full_history=full_history, skip_past=skip_past, resolve_dar=False)
    old_classes = _old_cache_lora_classes(lc)
    new_classes = lc._cache_lora_classes()
    assert new_classes == old_classes


def old_cache_lora_itsystems(self):
    # IT-systems are eternal i MO and does not need a historic dump
    params = {'bvn': '%'}
    url = '/organisation/itsystem'
    itsystem_list = self._perform_lora_lookup(url, params, skip_history=True, unit="itsystem")

    itsystems = {}
    for itsystem in tqdm(itsystem_list, desc="Processing itsystem", unit="itsystem"):
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

@skip
@pytest.mark.parametrize("full_history", [True, False])
@pytest.mark.parametrize("skip_past", [True, False])
def test_itsystems_equivalence(full_history, skip_past):
    lc = LoraCache(full_history=full_history, skip_past=skip_past, resolve_dar=False)
    new_itsystems = lc._cache_lora_itsystems()
    old_itsystems = old_cache_lora_itsystems(lc)
    assert new_itsystems == old_itsystems


def old_cache_lora_users(self):
    params = {'bvn': '%'}
    url = '/organisation/bruger'
    user_list = self._perform_lora_lookup(url, params, unit="user")

    relevant = {
        "attributter": ("brugeregenskaber", "brugerudvidelser"),
        "relationer": ("tilknyttedepersoner", "tilhoerer"),
        "tilstande": ("brugergyldighed",),
    }

    users = {}
    for user in tqdm(user_list, desc="Processing user", unit="user"):
        uuid = user['id']
        users[uuid] = []

        effects = list(self._get_effects(user, relevant))
        for effect in effects:
            from_date, to_date = self._from_to_from_effect(effect)
            if from_date is None and to_date is None:
                continue
            reg = effect[2]

            tilknyttedepersoner = reg['relationer']['tilknyttedepersoner']
            if len(tilknyttedepersoner) == 0:
                continue
            cpr = tilknyttedepersoner[0]['urn'][-10:]

            egenskaber = reg['attributter']['brugeregenskaber']
            if len(egenskaber) == 0:
                continue
            egenskaber = egenskaber[0]

            udv = reg['attributter']['brugerudvidelser']
            if len(udv) == 0:
                continue
            udv = udv[0]

            user_key = egenskaber.get('brugervendtnoegle', '')
            fornavn = udv.get('fornavn', '')
            efternavn = udv.get('efternavn', '')
            kaldenavn_fornavn = udv.get('kaldenavn_fornavn', '')
            kaldenavn_efternavn = udv.get('kaldenavn_efternavn', '')
            users[uuid].append(
                {
                    'uuid': uuid,
                    'cpr': cpr,
                    'user_key': user_key,
                    'fornavn': fornavn,
                    'efternavn': efternavn,
                    'navn': ' '.join([fornavn, efternavn]).strip(),
                    'kaldenavn_fornavn': kaldenavn_fornavn,
                    'kaldenavn_efternavn': kaldenavn_efternavn,
                    'kaldenavn': ' '.join([kaldenavn_fornavn,
                                           kaldenavn_efternavn]).strip(),
                    'from_date': from_date,
                    'to_date': to_date
                }
            )
    return users

@skip
@pytest.mark.parametrize("full_history", [True, False])
@pytest.mark.parametrize("skip_past", [True, False])
def test_users_equivalence(full_history, skip_past):
    lc = LoraCache(full_history=full_history, skip_past=skip_past, resolve_dar=False)
    new_users = lc._cache_lora_users()
    old_users = old_cache_lora_users(lc)
    assert new_users == old_users


def _old_cache_lora_it_connections(self):
    params = {'gyldighed': 'Aktiv', 'funktionsnavn': 'IT-system'}
    url = '/organisation/organisationfunktion'
    it_connection_list = self._perform_lora_lookup(url, params, unit="it connection")

    it_connections = {}
    for it_connection in tqdm(it_connection_list, desc="Processing it connection", unit="it connection"):
        uuid = it_connection['id']
        it_connections[uuid] = []

        relevant = {
            'relationer': ('tilknyttedeenheder', 'tilknyttedebrugere',
                           'tilknyttedeitsystemer'),
            'attributter': ('organisationfunktionegenskaber',),
            # "tilstande": ("organisationfunktiongyldighed",)  # bug in old cache; is needed for equivalence
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


#@skip
@pytest.mark.parametrize("full_history", [True, False])
@pytest.mark.parametrize("skip_past", [True, False])
def test_itconnections_equivalence(full_history, skip_past):
    lc = LoraCache(full_history=full_history, skip_past=skip_past, resolve_dar=False)
    new_itconnections = lc._cache_lora_it_connections()
    old_itconnections = _old_cache_lora_it_connections(lc)
    assert new_itconnections == old_itconnections


def _old_cache_lora_related(self):
    params = {'gyldighed': 'Aktiv', 'funktionsnavn': 'Relateret Enhed'}
    url = '/organisation/organisationfunktion'
    related_list = self._perform_lora_lookup(url, params, unit="related")
    related = {}
    for relate in tqdm(related_list, desc="Processing related", unit="related"):
        uuid = relate['id']
        related[uuid] = []

        relevant = {
            'relationer': ('tilknyttedeenheder',),
            'attributter': ()
        }

        effects = self._get_effects(relate, relevant)
        for effect in effects:
            from_date, to_date = self._from_to_from_effect(effect)
            if from_date is None and to_date is None:
                continue

            rel = effect[2]['relationer']
            unit1_uuid = rel['tilknyttedeenheder'][0]['uuid']
            unit2_uuid = rel['tilknyttedeenheder'][1]['uuid']
            print(len(rel['tilknyttedeenheder']))
            related[uuid].append(
                {
                    'uuid': uuid,
                    'unit1_uuid': unit1_uuid,
                    'unit2_uuid': unit2_uuid,
                    'from_date': from_date,
                    'to_date': to_date
                }
            )
    return related


@skip
@pytest.mark.parametrize("full_history", [True, False])
@pytest.mark.parametrize("skip_past", [True, False])
def test_related_equivalence(full_history, skip_past):
    lc = LoraCache(full_history=full_history, skip_past=skip_past, resolve_dar=False)
    new_related = lc._cache_lora_related()
    old_related = _old_cache_lora_related(lc)
    # test fails because LoRa sometimes sorts the relationship tuple differently
    assert new_related == old_related
