import csv
import datetime
import hashlib
import json
import logging
import pathlib
import pickle
import uuid
from logging.handlers import RotatingFileHandler

import requests
from anytree import Node
from chardet.universaldetector import UniversalDetector

INFO_LEVEL = 20
LOG_FILE = 'udvalg.log'
BASE_URL = 'http://localhost:5000/service/'
CACHE = {}
SESSION = None 

logger = logging.getLogger()
log_format = logging.Formatter(
    '%(asctime)s | %(name)s |  %(levelname)s: %(message)s'
)
logger.setLevel(logging.DEBUG)
activity_log_handler = RotatingFileHandler(
    filename=LOG_FILE,
    maxBytes=1000000
)
activity_log_handler.setFormatter(log_format)
activity_log_handler.setLevel(INFO_LEVEL)
logger.addHandler(activity_log_handler)


def _find_class(find_facet, find_class):
    if find_class in CACHE:
        return CACHE[find_class]
    uuid = None
    url = BASE_URL + 'o/{}/f/{}'
    response = SESSION.get(url.format(ROOT, find_facet)).json()
    for actual_class in response['data']['items']:
        if actual_class['user_key'] == find_class:
            uuid = actual_class['uuid']
            CACHE[find_class] = uuid
    if uuid:
        return uuid
    raise Exception("Der blev ikke fundet et uuid for denne" )


def _mo_lookup(uuid, details=''):
    if not details:
        url = BASE_URL + 'e/{}'
    else:
        url = BASE_URL + 'e/{}/details/' + details
    response = SESSION.get(url.format(uuid))
    return(response.json())


def _find_org():
    url = BASE_URL + 'o'
    response = SESSION.get(url).json()
    assert(len(response) == 1)
    uuid = response[0]['uuid']
    return uuid

def _load_csv(file_name):
    rows = []
    detector = UniversalDetector()
    with open(file_name, 'rb') as csvfile:
        for row in csvfile:
            detector.feed(row)
            if detector.done:
                break
    detector.close()
    encoding = detector.result['encoding']

    with open(file_name, encoding=encoding) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        for row in reader:
            rows.append(row)
    return rows


def generate_uuid(value, org_name):
    """
    Code almost identical to this also lives in the Opus importer.
    """
    base_hash = hashlib.md5(org_name.encode())

    base_digest = base_hash.hexdigest()
    base_uuid = uuid.UUID(base_digest)

    combined_value = (str(base_uuid) + str(value)).encode()
    value_hash = hashlib.md5(combined_value)
    value_digest = value_hash.hexdigest()
    value_uuid = str(uuid.UUID(value_digest))
    return value_uuid


def _create_mo_ou(name, parent, org_type, bvn):
    uuid = generate_uuid(bvn, ROOT)
    ou_type = _find_class(find_facet='org_unit_type', find_class=org_type)
    ou_level = _find_class(find_facet='org_unit_level', find_class='MED-enhed')
    if parent == 'root':
        parent = ROOT
    payload = {
        'uuid': uuid,
        'user_key': str(bvn),
        'name': '{} {}'.format(org_type, name),
        'org_unit_type': {'uuid': ou_type},
        'org_unit_level':{'uuid': ou_level},
        'parent': {'uuid': parent},
        'validity': {'from': '1930-01-01',
                     'to':  None}
    }

    url = BASE_URL + 'ou/create'
    params = {'force': 1}
    response = SESSION.post(url, json=payload, params=params)
    uuid = response.json()
    return uuid


def _create_mo_association(user, org_unit, association_type, from_string):
    response = _mo_lookup(user, details='engagement')
    if response:
        job_function = response[0]['job_function']['uuid']
        payload = [
            {
                'type': 'association',
                'org_unit': {'uuid': org_unit},
                'person': {'uuid': user},
                'association_type': {'uuid': association_type},
                'job_function': {'uuid': job_function},
                'validity': {
                    'from': from_string,
                    'to': None
                }
            }
        ]
        url = BASE_URL + 'details/create'
        params = {'force': 1}

        response = SESSION.post(url, json=payload, params=params)
        response.raise_for_status()
        uuid = response.json()
        return uuid
    else:
        logger.warning('User {} has no engagements'.format(user))
        return None


def _create_mo_role(user, org_unit, role_type, from_string):
    response = _mo_lookup(user, details='engagement')
    if response:
        job_function = response[0]['job_function']['uuid']
        payload = [
            {
                'type': 'role',
                'org_unit': {'uuid': org_unit},
                'person': {'uuid': user},
                'role_type': {'uuid': role_type},
                'job_function': {'uuid': job_function},
                'validity': {
                    'from': from_string,
                    'to': None
                }
            }
        ]
        url = BASE_URL + 'details/create'
        params = {'force': 1}
        response = SESSION.post(url, json=payload, params=params)
        response.raise_for_status()
        uuid = response.json()
        return uuid
    else:
        logger.warning('User {} has no engagements'.format(user))
        return None


def create_udvalg(nodes, file_name):
    rows = _load_csv(file_name)
    for row in rows:
        if ('Formand' in row) and (row['Formand'] == '1'):
            association_type = _find_class('association_type', 'MED-Formand')
        elif ('Næstformand' in row) and (row['Næstformand'] == '1'):
            association_type = _find_class('association_type', 'MED-Næstformand')
        elif ('suppleant' in row) and (row['suppleant'] == '1'):
            association_type = _find_class('association_type', 'MED-Medlem-Suppleant')    
        else:
            association_type = _find_class('association_type', 'MED-Medlem')

        if ('TR' in row) and (row['TR'] == '1'):
            role_type = _find_class('role_type', 'MED-Tillidsrepræsentant')
        else:
            role_type = None

        org_id = int(row['Id'])
        uuid = row['BrugerUUID']
        try:
            from_string = datetime.datetime.strftime(
                datetime.datetime.strptime(row['StartDato'], '%d-%b-%y'),
                '%Y-%m-%d'
            )
        except ValueError:
            from_string = '1930-01-01'
        except TypeError:
            from_string = '1930-01-01'

        if uuid:
            nodes[uuid] = Node(row['Fornavn'] + ' ' + row['Efternavn'],
                               uuid=uuid,
                               org_type=row['OrgType'],
                               parent=nodes[org_id])
            _create_mo_association(uuid,
                                   nodes[org_id].uuid,
                                   association_type,
                                   from_string)
            if role_type:
                _create_mo_role(uuid, nodes[org_id].uuid, role_type, from_string)

        else:
            logger.warning(
                'Error: {} {}, UUID: {}'.format(row['Fornavn'],
                                               row['Efternavn'],
                                               row['BrugerUUID'])
            )
    return nodes


def create_tree(file_name):
    nodes = {}
    rows = _load_csv(file_name)
    while rows:
        new = {}
        remaining_nodes = []
        for row in rows:
            org_type = row['OrgType'].strip()
            id_nr = int(row['Id'])
            parent = int(row['ParentID']) if row['ParentID'] else None
            if parent is None:
                uuid = _create_mo_ou(row['OrgEnhed'], parent='root',
                                     org_type=org_type, bvn=id_nr)
                new[id_nr] = Node(row['OrgEnhed'],
                                  uuid=uuid, org_type=org_type)
            elif parent in nodes:
                uuid = _create_mo_ou(row['OrgEnhed'],
                                     parent=nodes[parent].uuid,
                                     org_type=org_type, bvn=id_nr)
                new[id_nr] = Node(row['OrgEnhed'],
                                  uuid=uuid, org_type=org_type,
                                  parent=nodes[parent])
            else:
                remaining_nodes.append(row)
        rows = remaining_nodes
        nodes.update(new)
    return nodes


if __name__ == '__main__':
    logger.info('Program started')

    settingsfile = pathlib.Path("settings") / "settings.json"
    settings = json.loads(settingsfile.read_text())


    SESSION = requests.Session()
    if settings.get("crontab.SAML_TOKEN", None) is not None:
        SESSION.headers["SESSION"] = settings["crontab.SAML_TOKEN"]


    ROOT = _find_org()

    orgtyper_file = '/opt/settings/MED-org/MED-Organisation.csv'
    med_medlemmer_file = '/opt/settings/MED-org/MED-Medlemmer.csv'
    
    if True:
        nodes = create_tree(orgtyper_file)
        with open('nodes.p', 'wb') as f:
            pickle.dump(nodes, f, pickle.HIGHEST_PROTOCOL)

    with open('nodes.p', 'rb') as f:
        nodes = pickle.load(f)


    logger.info('Create MED')
    nodes = create_udvalg(nodes, med_medlemmer_file)

    # root = min(nodes.keys())
    # from anytree import RenderTree
    # print(RenderTree(nodes[root]))
    logger.info('Program completed.')
