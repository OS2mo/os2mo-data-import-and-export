import csv
import pickle
import requests
from anytree import Node
from chardet.universaldetector import UniversalDetector


BALLERUP = '2f41ace9-4c69-4d82-b26e-2d2ae2ff0e0b'
queries = {}


def mo_lookup(uuid):
    url = 'http://mora_dev_tools/service/e/{}'.format(uuid)
    response = requests.get(url)
    return(response.json())


def search_mo_name(name, user_key):
    url = 'http://mora_dev_tools/service/o/{}/e?query={}'
    response = requests.get(url.format(BALLERUP, name))
    result = response.json()
    if len(result['items']) == 1:
        return result['items'][0]['uuid']
    # Did not succeed with simple search, try user_Key
    response = requests.get(url.format(BALLERUP, user_key))
    result = response.json()
    for employee in result['items']:
        uuid = employee['uuid']
        mo_user = mo_lookup(uuid)
        if mo_user['user_key'] == user_key:
            return(employee['uuid'])
    # Still no success, give up and return None
    return None


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
        reader = csv.DictReader(csvfile, delimiter=',')
        for row in reader:
            rows.append(row)
    return rows


def _create_mo_ou(name, parent):
    ou_type = '656d6551-e3e4-4c8e-bb0f-9a369d9334d2'
    if parent is 'root':
        parent = BALLERUP
    # Chceck org_unit_type, might need a new facet
    payload = {'name': name,
               'brugervendtnoegle': name,
               'org_unit_type': {'uuid': ou_type},
               'parent': {'uuid': parent},
               'validity': {'from': '2010-01-01',
                            'to': '2100-01-01'}}
    url = 'http://mora_dev_tools/service/ou/create'
    response = requests.post(url, json=payload)
    uuid = response.json()
    return uuid


def _create_mo_association(user, org_unit):
    # Chceck org_unit_type, might need a new facet
    url = 'http://mora_dev_tools/service/e/{}/details/engagement'
    response = requests.get(url.format(user)).json()
    job_function = response[0]['job_function']['uuid']
    payload = [{'type': 'association',
                'org_unit': {'uuid': org_unit},
                'person': {'uuid': user},
                'association_type': {
                    'uuid': '74de4089-56f9-4a6e-a044-1758ff941896'
                },
                'job_function': {'uuid': job_function},
                'validity': {'from': '2010-01-01', 'to': '2100-01-01'}}]
    url = 'http://mora_dev_tools/service/details/create'
    response = requests.post(url, json=payload)
    uuid = response.json()
    return uuid


def _read_udvalg(file_name, nodes):
    rows = _load_csv(file_name)
    for row in rows:
        org_id = int(row['Id'])
        uuid = search_mo_name(row['Fornavn'] + ' ' + row['Efternavn'],
                              row['BrugerID'])
        if uuid:
            nodes[uuid] = Node(row['Fornavn'] + ' ' + row['Efternavn'],
                               uuid=uuid,
                               org_type=row['OrgType'],
                               parent=nodes[org_id])
            _create_mo_association(uuid, nodes[org_id].uuid)
        else:
            print('Error: {} {}, bvn: {}'.format(row['Fornavn'],
                                                 row['Efternavn'],
                                                 row['BrugerID']))
    return nodes


def read_tree(file_name):
    nodes = {}
    rows = _load_csv(file_name)
    while rows:
        new = {}
        remaining_nodes = []
        for row in rows:
            id_nr = int(row['Id'])
            parent = int(row['ParentID']) if row['ParentID'] else None
            if parent is None:
                uuid = _create_mo_ou(row['OrgEnhed'], parent='root')
                new[id_nr] = Node(row['OrgEnhed'],
                                  uuid=uuid,
                                  org_type=row['OrgType'].strip())
            elif parent in nodes:
                uuid = _create_mo_ou(row['OrgEnhed'],
                                     parent=nodes[parent].uuid)
                new[id_nr] = Node(row['OrgEnhed'],
                                  uuid=uuid,
                                  org_type=row['OrgType'].strip(),
                                  parent=nodes[parent])
            else:
                remaining_nodes.append(row)
        rows = remaining_nodes
        nodes.update(new)
    return nodes


def read_AMR_udvalg(nodes):
    filename = 'AMR-medlemmer.csv'
    nodes = _read_udvalg(filename, nodes)
    return nodes


def read_MED_udvalg(nodes):
    filename = 'MED-medlemmer.csv'
    nodes = _read_udvalg(filename, nodes)
    return nodes


if __name__ == '__main__':
    from anytree import RenderTree

    if False:
        nodes = read_tree('OrgTyper.csv')
        with open('nodes.p', 'wb') as f:
            pickle.dump(nodes, f, pickle.HIGHEST_PROTOCOL)

    with open('nodes.p', 'rb') as f:
        nodes = pickle.load(f)

    # nodes = read_AMR_udvalg(nodes)
    nodes = read_MED_udvalg(nodes)

    # root = min(nodes.keys())
    # print(RenderTree(nodes[root]))
