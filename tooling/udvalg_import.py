import csv
import requests
from anytree import Node
from chardet.universaldetector import UniversalDetector


BALLERUP = '3a87187c-f25a-40a1-8d42-312b2e2b43bd'
queries = {}


def mo_lookup(uuid):
    url = 'http://mora_dev_tools/service/e/{}'.format(uuid)
    response = requests.get(url)
    return(response.json())


def search_mo_name(name, user_key):
    return_uuid = None
    url = 'http://mora_dev_tools/service/o/{}/e?query='.format(BALLERUP) + name
    response = requests.get(url)
    result = response.json()

    for employee in result['items']:
        uuid = employee['uuid']
        mo_user = mo_lookup(uuid)
        if mo_user['user_key'] == user_key:
            return_uuid = uuid
    return return_uuid


def search_mo_unit(name, current):
    return_uuid = None
    base_url = 'http://mora_dev_tools/service/'
    if current.name is 'root':
        url = base_url + 'o/{}/children'.format(BALLERUP)
    else:
        url = base_url + 'ou/{}/children'.format(current.name)

    if url not in queries:
        response = requests.get(url)
        result = response.json()
        queries[url] = result
    else:
        result = queries[url]

    for ou in result:
        if ou['name'] == name:
            return_uuid = ou['uuid']
    return return_uuid


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
    # Chceck org_unit_type, might need a new facet
    payload = {'name': name,
               'brugervendtnoegle': name,
               'org_unit_type': '656d6551-e3e4-4c8e-bb0f-9a369d9334d2',
               'parent': {'uuid': parent}}
    return payload


def _read_udvalg(file_name, path_columns, udvalgs_type, nodes):
    current_node = nodes['root']
    rows = _load_csv(file_name)

    for row in rows:
        for field in path_columns:
            node = row[field]
            if not node:
                break  # Field is empty, step out of the loop

            mo_ou_uuid = search_mo_unit(node, current_node)
            if mo_ou_uuid is None:
                print('No such unit ' + node)
                print(_create_mo_ou(node, current_node.name))
            elif mo_ou_uuid in nodes:
                current_node = nodes[mo_ou_uuid]
            else:
                nodes[mo_ou_uuid] = Node(mo_ou_uuid,
                                         parent=current_node,
                                         real_name=node)
                current_node = nodes[mo_ou_uuid]

        if row['Fornavn']:
            uuid = search_mo_name(row['Fornavn'] + ' ' + row['Efternavn'],
                                  row['BrugerID'])
            if uuid:
                print(uuid)
                nodes[uuid] = Node(row['Fornavn'] + ' ' + row['Efternavn'],
                                   uuid=uuid,
                                   udvalgs_type=udvalgs_type,
                                   parent=current_node)
        current_node = nodes['root']

    return nodes


def read_AMR_udvalg(nodes):
    path_columns = ['Hoved-MED-niveau1', 'Center-MED-niveau3',
                    'Lokal-MED-niveau3', 'AMR-Gruppe']
    filename = '/home/robertj/ballerup_tmp/AMR-udvalg.csv'
    nodes = _read_udvalg(filename, path_columns, 'AMR', nodes)
    return nodes


def read_MED_udvalg(nodes):
    path_columns = ['Hoved-M-niveau1', 'Center-MED-niveau2',
                    'Lokal-MED-niveau3']
    filename = '/home/robertj/ballerup_tmp/MED-udvalg.csv'
    nodes = _read_udvalg(filename, path_columns, 'MED', nodes)
    return nodes


if __name__ == '__main__':
    from anytree import RenderTree

    nodes = {'root': Node('root', parent=None)}
    # nodes = read_AMR_udvalg(nodes)
    nodes = read_MED_udvalg(nodes)

    print(RenderTree(nodes['root']))
