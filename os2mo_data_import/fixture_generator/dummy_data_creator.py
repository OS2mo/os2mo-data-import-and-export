""" Create dummy data to populate MO """
import pickle
import random
import pathlib
import requests
from enum import Enum
from datetime import datetime
from datetime import timedelta
from uuid import uuid5, NAMESPACE_DNS
from anytree import Node, PreOrderIter

CLASSES = {
    'engagement_job_function': [
        'Udvikler', 'Specialkonsulent', 'Ergoterapeut', 'Udviklingskonsulent',
        'Specialist', 'Jurist', 'Personalekonsulent', 'Lønkonsulent',
        'Kontorelev', 'Ressourcepædagog', 'Pædagoisk vejleder',
        'Skolepsykolog', 'Støttepædagog', 'Bogopsætter', 'Timelønnet lærer',
        'Pædagogmedhjælper', 'Teknisk Servicemedarb.', 'Lærer/Overlærer'
    ],
    'association_type': [
        'Formand', 'Leder', 'Medarbejder', 'Næstformand', 'Projektleder',
        'Projektgruppemedlem', 'Teammedarbejder'
    ],
    'org_unit_type': [
        'Afdeling', 'Institutionsafsnit', 'Institution', 'Fagligt center',
        'Direktørområde'
    ],
    'org_unit_level': ['N1', 'N2', 'N3', 'N4', 'N5', 'N6', 'N7', 'N8'],
    'responsibility': [
        'Personale: ansættelse/afskedigelse',
        'Beredskabsledelse',
        'Personale: øvrige administrative opgaver',
        'Personale: Sygefravær',
        'Ansvar for bygninger og arealer',
        'Personale: MUS-kompetence'
    ],
    'manager_type': [
        'Direktør', 'Distriktsleder', 'Beredskabschef', 'Sekretariatschef',
        'Systemadministrator', 'Områdeleder', 'Centerchef', 'Institutionsleder'
    ],
    'role_type': [
        'Tillidsrepræsentant', 'Ergonomiambasadør', 'Ansvarlig for sommerfest'
    ],
    'leave_type': [
        'Barselsorlov', 'Forældreorlov', 'Orlov til pasning af syg pårørende'
    ],
    'employee_address_type': [
        ('AdressePostEmployee', 'Postadresse', 'DAR'),
        ('PhoneEmployee', 'Telefon', 'PHONE'),
        ('LocationEmployee', 'Lokation', 'TEXT'),
        ('EmailEmployee', 'Email', 'EMAIL')
    ],
    'manager_address_type': [
        ('EmailManager', 'Email', 'EMAIL'),
        ('TelefonManager', 'Telefon', 'PHONE'),
        ('AdressePostManager', 'Adresse', 'DAR'),
        ('WebManager', 'Webadresse', 'TEXT')
    ],
    'org_unit_address_type': [
        ('AddressMailUnit', 'Postadresse', 'DAR'),
        ('AdressePostRetur', 'Returadresse', 'DAR'),
        ('AdresseHenvendelsessted', 'Henvendelsessted', 'DAR'),
        ('LocationUnit', 'Lokation', 'TEXT'),
        ('Skolekode', 'Skolekode', 'TEXT'),
        ('Formålskode', 'Formålskode', 'TEXT'),
        ('Afdelingskode', 'Afdelingskode', 'TEXT'),
        ('EmailUnit', 'Email', 'EMAIL'),
        ('PhoneUnit', 'Telefon', 'PHONE'),
        ('FaxUnit', 'Fax', 'PHONE'),
        ('EAN', 'EAN-nummer', 'EAN'),
        ('WebUnit', 'Webadresse', 'WWW'),
        ('p-nummer', 'P-nummer', 'PNUMBER')
    ],
    'manager_level': ['Niveau 1', 'Niveau 2', 'Niveau 3', 'Niveau 4'],
    'time_planning': ['Arbejdstidsplaner', 'Dannes ikke', 'Tjenestetid'],
    'engagement_type': ['Ansat'],
    'visibility': [
        ('Ekstern', 'Må vises eksternt', 'PUBLIC'),
        ('Intern', 'Må vises internt', 'INTERNAL'),
        ('Hemmelig', 'Hemmelig', 'SECRET')
    ],
    'primary_type': [
        ('explicitly-primary', 'Manuelt primær ansættelse', '5000'),
        ('primary', 'Primær', '3000'),
        ('non-primary', 'Ikke-primær ansættelse', '0')
    ]
}

IT_SYSTEMS = ['Active Directory', 'SAP', 'Office365', 'Plone', 'OpenDesk']

START_DATE = '1960-01-01'


def _path_to_names():
    """ Return a list of paths to the name-lists """
    path = pathlib.Path(__file__).resolve().parent
    path = path / 'navne'
    navne_list = [path / 'fornavne.txt',
                  path / 'mellemnavne.txt',
                  path / 'efternavne.txt']
    return navne_list


def _load_names(name_file):
    """ Load a weighted list of names
    :param name_file: Name of the text file with names
    :return: A weighted list of names
    """
    with name_file.open('r') as f:
        names_file = f.read()
    name_lines = names_file.split('\n')
    names = []
    for name_set in name_lines:
        try:
            parts = name_set.split('\t')
            if parts[2].find(',') > 0:
                subnames = parts[2].split(',')
                for subname in subnames:
                    names.append([int(parts[1]), subname])
            else:
                names.append([int(parts[1]), parts[2]])
        except IndexError:
            pass
    return names


def _number(length):
    """ Create a random number of some length
    Danish phone numbers should be 8 characters.
    EAN numbers should be 13 characters.
    :param length: The lengh of the number string.
    :return: A random phone number
    """
    number = str(random.randrange(1, 9))
    for i in range(0, length - 1):
        number += str(random.randrange(0, 9))
    return number


def _cpr(time_from=None):
    """ Create a random valid cpr.
    :return: A valid cpr number
    """
    mod_11_table = [4, 3, 2, 7, 6, 5, 4, 3, 2]
    days_in_month = {
        '01': 31, '02': 28, '03': 31, '04': 30,
        '05': 31, '06': 30, '07': 31, '08': 31,
        '09': 30, '10': 31, '11': 30, '12': 31
    }
    days_to_choose = sorted(days_in_month.keys())
    month = list(days_to_choose)[random.randrange(0, 12)]
    day = str(random.randrange(1, 1 + days_in_month[month])).zfill(2)

    if time_from is not None:
        max_year = min(99, time_from.year - 1900 - 18)
        year = str(random.randrange(40, max_year))
    else:
        year = str(random.randrange(40, 99))
    digit_7 = str(random.randrange(0, 4))  # 1900 < Birth year < 2000

    valid_10 = False
    while not valid_10:
        digit_8_9 = str(random.randrange(10, 100))
        cpr_number = day + month + year + digit_7 + digit_8_9
        mod_11_sum = 0
        for i in range(0, 9):
            mod_11_sum += int(cpr_number[i]) * mod_11_table[i]
        remainder = mod_11_sum % 11

        if remainder == 0:
            digit_10 = '0'
        else:
            digit_10 = str(11 - remainder)
        valid_10 = remainder != 1
    cpr_number = cpr_number + digit_10
    return cpr_number


def _name_to_email(name):
    email = name.replace(' ', '_')
    return email


def _name_to_host(name):
    """ Turn an org name into a valid hostname """
    if name.find(' ') > -1:
        name = name[:name.find(' ')]
    name = name.lower()
    name = name.replace('æ', 'ae')
    name = name.replace('ø', 'o')
    name = name.replace('å', 'a')
    name = name + '.dk'
    return name


Size = Enum('Size', 'Small Normal Large')


class CreateDummyOrg(object):
    """ Create a dummy organisation to use as test data
    Users are randomly created within the sample space provided by
    the above constants. The units and addresses in the organisation
    is based on the given municipality code to make the generated data
    look realistic.
    """

    def __init__(self, municipality_code, name, path_to_names,
                 root_name='root', predictable_uuids=False):
        self.global_start_date = datetime.strptime(START_DATE, '%Y-%m-%d')
        self.classes = CLASSES
        self.it_systems = IT_SYSTEMS
        self.nodes = {}
        self.name = name
        self.root_name = root_name
        self.predictable_uuids = predictable_uuids
        try:
            with open(str(municipality_code) + '.p', 'rb') as file_handle:
                self.adresser = pickle.load(file_handle)
        except OSError:
            addr = 'http://dawa.aws.dk/adresser?kommunekode={}&struktur=mini'
            r = requests.get(addr.format(municipality_code))
            self.adresser = r.json()
            with open(str(municipality_code) + '.p', 'wb') as file_handle:
                pickle.dump(self.adresser, file_handle)

        self.names = {'first': _load_names(path_to_names[0]),
                      'middle': _load_names(path_to_names[1]),
                      'last': _load_names(path_to_names[2])}

        address = self._address()
        self.nodes[self.root_name] = Node(
            name,
            address=address[0],
            url='www.{}'.format(_name_to_host(self.name)),
            email='info@{}'.format(_name_to_host(self.name)),
            time_planning='Dannes ikke',
            p_number=_number(10),
            ean=_number(13),
            unit_level='N1',
            place_of_contact=address[1],
            location=None,
            type='ou',
            key=self.root_name,
            uuid=str(uuid5(NAMESPACE_DNS, self.root_name))
        )
        # Used to keep track of used user_keys to keep them unique
        self.used_user_keys = []

    def _pick_name_from_list(self, name_type):
        """
        Pick a name
        :param name_type: Type of name, first, middle or last
        :return: A name
        """
        names = self.names[name_type]
        total_weight = 0
        for name in names:
            total_weight += name[0]
        weight = 0
        stop_weight = random.randrange(total_weight)
        for name in names:
            weight += name[0]
            if weight > stop_weight:
                break
        return name[1]

    def _postdistrikter(self):
        """
        Create a list of all unique postal areas
        :return: List of all unique postal areas
        """
        postdistrikter = []
        for adresse in self.adresser:
            if adresse['postnrnavn'] not in postdistrikter:
                postdistrikter.append(adresse['postnrnavn'])
        return postdistrikter

    def _address(self):
        """ Create a Danish adresse """
        addr_index = random.randrange(len(self.adresser))
        addr = self.adresser[addr_index]
        address = {'postnummer': addr['postnr'],
                   'postdistrikt': addr['postnrnavn'],
                   'adresse': addr['vejnavn'] + ' ' + addr['husnr'],
                   'dar-uuid': addr['id']}

        if random.random() > 0.8:
            addr = self.adresser[addr_index + 1]
            place_of_contact = {'dar-uuid': addr['id']}
        else:
            place_of_contact = address
        return address, place_of_contact

    def _create_org_level(self, org_list, parent, level):
        """
        Create a dict with names, adresses and parents.
        :param org_list: List of names of the organisation.
        :return: A flat dict with name, random adress and room for sub-units.
        """
        unit_level = self.classes['org_unit_level'][level]
        uuid_list = []
        for org_tuple in org_list:
            if not isinstance(org_tuple, tuple):
                org_tuple = (org_tuple, None)

            org = org_tuple[0]
            if org_tuple[1] and self.predictable_uuids:
                uuid = org_tuple[1]
            else:
                # This somewhat complicated way of making uuids ensures that
                # it is possible to make consistent uuid's by freezing the
                # random seed.
                uuid = str(uuid5(NAMESPACE_DNS, str(random.random())))
            uuid_list.append(uuid)

            addresses = self._address()
            if random.random() > 0.75:
                location = 'Bygning {}'.format(random.randrange(1, 20))
            else:
                location = None

            time_planning = random.choice(self.classes['time_planning'])
            self.nodes[uuid] = Node(
                org,
                address=addresses[0],
                place_of_contact=addresses[1],
                location=location,
                time_planning=time_planning,
                email='{}@{}'.format(_name_to_email(org), _name_to_host(self.name)),
                ean=_number(13),
                unit_level=unit_level,
                p_number=_number(10),
                url=None,
                parent=parent,
                key=str(uuid),
                uuid=str(uuid),
                type='ou'
            )
        return uuid_list

    def create_name(self, return_user_key=False):
        """
        Create a full name.
        :return: The full name as a string.
        """
        first = self._pick_name_from_list('first')

        middle = ''
        if random.random() > 0.3:
            middle = middle + ' ' + self._pick_name_from_list('middle')
        if random.random() > 0.9:
            middle = middle + ' ' + self._pick_name_from_list('middle')

        last = self._pick_name_from_list('last')
        name = (first + middle, last)
        user_key = first + last[0]
        i = 0
        while user_key in self.used_user_keys:
            i = i + 1
            user_key = first[0:i+2] + last[0:i]
            if i > len(last):
                user_key = user_key + str(random.randrange(1, 999))
        self.used_user_keys.append(user_key)

        if return_user_key:
            return name, user_key
        else:
            return name

    def _create_user(self, name, user_key, time_from, time_to, cpr=None, manager=[]):
        """
        Create a primitive user payload. Phone, job_function, it_systems
        and address will be randomized.
        :param name: Name of the user.
        :param user_key: user key of the user.
        :param time_from: Start date of the engagement.
        :param time_to: End date of the engagement.
        :param cpr: cpr of the user, of none a random will be chosen.
        :param manager: List of manager responsibilities, if empty the user is
        not a manager.
        :return: A dict with information about the user
        """
        it_systems = random.sample(self.it_systems, random.randrange(0, 3))
        job_function = random.choice(self.classes['engagement_job_function'])
        secret_phone = random.choice(self.classes['visibility'])
        host = _name_to_host(self.name)
        if cpr is None:
            cpr = _cpr(time_from)

        if random.random() > 0.75:
            location = 'Bygning {}'.format(random.randrange(1, 20))
        else:
            location = None

        user = {'fra': time_from,
                'til': time_to,
                'brugervendtnoegle': user_key,
                'givenname': name[0],
                'surname': name[1],
                'email': user_key.lower() + '@' + host,
                'secret_phone': secret_phone[0],
                'location': location,
                'phone': _number(8),
                'cpr': cpr,
                'job_function': job_function,
                'manager': manager,
                'it_systemer': it_systems,
                'address': self._address()[0]}
        return user

    def create_user(self, manager=[], multiple_employments=False):
        """
        Create a MO user with a random name and phone.
        :return: A dict with information about the user.
        """
        name, user_key = self.create_name(return_user_key=True)
        user = []
        if not multiple_employments:
            from_delta = timedelta(days=30 * random.randrange(0, 750))
            # Some employees will fail cpr-check. So be it.
            time_from = self.global_start_date + from_delta
            cpr = _cpr(time_from)
            if random.random() > 0.75:
                to_delta = timedelta(days=30 * random.randrange(100, 500))
                time_to = time_from + to_delta
            else:
                time_to = None
            user.append(self._create_user(name, user_key, time_from, time_to,
                                          cpr, manager))
        else:
            delta = timedelta(days=30 * random.randrange(0, 240))
            time_from = self.global_start_date + delta
            cpr = _cpr(time_from)
            for i in range(1, random.randrange(1, 15)):
                delta = timedelta(days=30 * random.randrange(0, 150))
                time_to = time_from + delta
                user.append(self._create_user(name, user_key,
                                              time_from, time_to, cpr))
                delta = timedelta(days=30 * random.randrange(0, 5))
                time_from = time_to + delta
            user.append(self._create_user(name, user_key, time_from, None, cpr))
        return user

    def create_org_func_tree(self, org_size=Size.Normal):
        """ Create an organisational structure, based on the municipality code.
        :param org_size: If 'Normal' a standard number of units will be made. If
        'Large' a  large number of units will be made in one of the the sub-trees for
        performance testing purposes. If 'Small' the a smaller amount of units is
        created, mainly to facilitate faster testing.
        """
        orgs = [('Borgmesterens Afdeling', 'b6c11152-0645-4712-a207-ba2c53b391ab'),
                ('Teknik og Miljø', '23a2ace2-52ca-458d-bead-d1a42080579f'),
                ('Skole og Børn', '7a8e45f7-4de0-44c8-990f-43c0565ee505'),
                ('Social og sundhed', 'a6773531-6c0a-4c7b-b0e2-77992412b610')]
        self._create_org_level(orgs, parent=self.nodes[self.root_name], level=0)

        keys = sorted(self.nodes.keys())  # Sort the keys to ensure test-cosistency
        for node in list(keys):
            org = self.nodes[node].name
            if not org_size == Size.Small:
                if org == 'Teknik og Miljø':
                    orgs = [('Kloakering', 'cf4daae1-4812-41f1-8c47-63a99e26aadf'),
                            ('Park og vej', '1a477478-41b4-4806-ac3a-e220760a0c89'),
                            ('Renovation', 'dac3b1ef-3d36-4464-9839-f611a4215cb5'),
                            ('Belysning', 'fe2d2ff4-45f8-4b19-8e1b-72d1c4914360'),
                            ('IT-Support', '8bf0c4ce-816e-41f9-99fe-057e0592d86d')]
                    uuids = self._create_org_level(orgs, self.nodes[node], level=1)
                    for uuid in uuids:
                        if random.random() > 0.5:
                            self._create_org_level(['Kantine'], self.nodes[uuid],
                                                   level=2)

                if org == 'Borgmesterens Afdeling':
                    orgs = [
                        ('Budget og Planlægning',
                         '1f06ed67-aa6e-4bbc-96d9-2f262b9202b5'),
                        ('HR og organisation',
                         '96a4715c-f4df-422f-a4b0-9dcc686753f7'),
                        ('Erhverv', 'e054559b-bc15-4203-bced-44375aed1555'),
                        ('Byudvikling', 'f1c20ee2-ecbb-4b74-b91c-66ef9831c5cd'),
                        ('IT-Support', '25e39a21-caef-4e96-ac90-7cc27173082e')
                    ]
                    self._create_org_level(orgs, self.nodes[node], level=1)

            if org == 'Skole og Børn':
                orgs = [
                    ('Social Indsats', '535ba446-d618-4e51-8dae-821d63e26560'),
                    ('IT-Support', '9b7b3dde-16c9-4f88-87cc-e03aa5b4e709')
                ]
                self._create_org_level(orgs, self.nodes[node], level=2)

                org = ['Skoler og børnehaver']
                uuid = self._create_org_level(org, self.nodes[node], level=3)[0]

                skoler = []
                for dist in self._postdistrikter():
                    skoler.append(dist + " skole")
                    if org_size == Size.Large:
                        for i in range(0, 25):
                            skoler.append(dist + " skole " + str(i))
                self._create_org_level(skoler, self.nodes[uuid], level=4)

                if not org_size == Size.Small:
                    børnehaver = [dist + " børnehus"
                                  for dist in self._postdistrikter()]
                    uuids = self._create_org_level(børnehaver, self.nodes[uuid],
                                                   level=4)
                    for uuid in uuids:
                        if random.random() > 0.5:
                            self._create_org_level(['Administration'],
                                                   self.nodes[uuid], level=5)
                        elif random.random() > 0.5:
                            self._create_org_level(
                                ['Administration', 'Teknisk Support'],
                                self.nodes[uuid], level=6
                            )

    def create_manager(self):
        """
        Create a user, that is also a manager.
        :return: The user object, including manager classes
        """
        antal_ansvar = len(CLASSES['responsibility'])
        ansvar_list = [0]
        ansvar_list += random.sample(range(1, antal_ansvar), 2)
        responsibility_list = []
        for i in ansvar_list:
            responsibility_list.append(CLASSES['responsibility'][i])
        user = self.create_user(manager=responsibility_list)
        user[0]['association'] = None
        user[0]['role'] = None
        return user

    def add_user_func(self, facet, node=None):
        """
        Add a function to a user, ie. a Role or an Association
        :param facet: The kind of function to add to the user
        :param node: If a node is given, this will be used for the unit
        otherwise a random unit is chocen
        :return: The payload to create the function
        """
        if node is not None:
            unit = node.key
        else:
            unit = random.choice(sorted(list(self.nodes.keys())))
        payload = None
        if random.random() > 0.6:
            payload = {
                'unit': unit,
                'type': random.choice(self.classes[facet])
            }
        return payload

    def add_users_to_tree(self, ou_size_scale, multiple_employments=False):
        new_nodes = {}
        for node in PreOrderIter(self.nodes[self.root_name]):
            size = ou_size_scale * (node.depth + 1)
            ran_size = random.randrange(round(size/4), size)
            for _ in range(0, ran_size):
                user = self.create_user(multiple_employments=multiple_employments)
                for eng in user:
                    eng['association'] = self.add_user_func('association_type')
                    eng['role'] = self.add_user_func('role_type', node)

                uuid = uuid5(NAMESPACE_DNS, str(random.random()))
                new_nodes[uuid] = {
                    'givenname': user[0]['givenname'],
                    'surname': user[0]['surname'],
                    'user': user,
                    'parent': node
                }

            # In version one we always add a single manager to a OU
            # This should be randomized and also sometimes be a vacant
            # position
            user = self.create_manager()
            uuid = uuid5(NAMESPACE_DNS, str(random.random()))
            new_nodes[uuid] = {
                'givenname': user[0]['givenname'],
                'surname': user[0]['surname'],
                'user': user,
                'parent': node
            }
        keys = sorted(new_nodes.keys())
        for key in list(keys):
            user_info = new_nodes[key]
            user_node = Node(name=user_info['user'][0]['givenname'],
                             givenname=user_info['user'][0]['givenname'],
                             surname=user_info['user'][0]['surname'],
                             user=user_info['user'], type='user',
                             parent=user_info['parent'])
            self.nodes[key] = user_node


if __name__ == '__main__':
    dummy_creator = CreateDummyOrg(825, 'Læsø Kommune',
                                   _path_to_names())
    dummy_creator.create_org_func_tree(org_size=Size.Normal)
    dummy_creator.add_users_to_tree(ou_size_scale=1, multiple_employments=False)

    # Example of iteration over all nodes:
    for node in PreOrderIter(dummy_creator.nodes['root']):

        if node.type == 'ou':
            print()
            print(node.name)  # Name of the ou
            if node.parent:
                print(node.parent.key)  # Key for parent unit
            print(node.address['dar-uuid'])

        if node.type == 'user':
            print()
            print('---')
            print('Given name: {}, Surname: {}'.format(
                node.givenname,
                node.surname
            ))
            print(node.parent.key)  # Key for parent unit
            user = node.user  # All user information is here
            for engagement in user:
                print(engagement['brugervendtnoegle'])
                print('Rolle: {}'.format(engagement['role']))
                print('Tilknytning: {}'.format(engagement['association']))
                print('Ansvar: {}'.format(engagement['manager']))
                print('Fra: {}. Til: {}'.format(engagement['fra'],
                                                engagement['til']))
