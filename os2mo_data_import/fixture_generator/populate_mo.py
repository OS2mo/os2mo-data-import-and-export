import sys
from fixture_generator import dummy_data_creator
from datetime import datetime
from click import command, option, Abort
from requests import Session
from anytree import PreOrderIter
from os2mo_data_import import ImportHelper
from fixture_generator.dummy_data_creator import Size


"""
This tools needs either 2 or 7 arguments:
First two arguments
* Municipality number.
* Municipality name.

Five further arguments (either all or none of these should be supplied):
* Scale: Scales the number of employees (default 4).
* Org size: Small, Normal, Large. If large, multiple employmens will be
made for each employee.  If small, only very few units will be created.
* Extra root: Add an extra root unit (default True).
* mox_base: Default http://localhost:5000
* mora_base: Default http://localhost:80

Example:
python3 populate_mo.py 825 "Læsø Kommune" 2 Small True
http://localhost:5000 http://localhost:80
"""


class CreateDummyOrg(object):
    def __init__(self, importer, municipality_code, name,
                 scale=1, org_size=Size.Normal, extra_root=True):
        self.data = self.create_dummy_data(municipality_code, name, scale, org_size)

        self.extra_data = self.create_dummy_data(municipality_code, name,
                                                 scale, org_size=Size.Small,
                                                 root_name='extra_root')
        self.importer = importer

        self.importer.add_organisation(
            identifier=self.data.nodes['root'].name,
            user_key=self.data.nodes['root'].name,
            municipality_code=municipality_code
        )

        self.create_classes()
        self.create_it_systems()

        for node in PreOrderIter(self.data.nodes['root']):
            if node.type == 'ou':
                self.create_ou(node)
            if node.type == 'user':
                self.create_user(node)

        if extra_root:
            for node in PreOrderIter(self.extra_data.nodes['extra_root']):
                self.extra_data.nodes['extra_root'].name = 'Lønorganisation'
                if node.type == 'ou':
                    self.create_ou(node)

                    for org_node in PreOrderIter(self.data.nodes['root']):
                        if ((org_node.name == node.name) and
                            ((org_node.parent.name == node.parent.name) or
                             node.parent.name == 'Lønorganisation')):

                            for sub_node in org_node.children:
                                if sub_node.type == 'user':
                                    pass

    def create_dummy_data(self, municipality_code, name, scale, org_size,
                          root_name='root'):
        name_path = dummy_data_creator._path_to_names()
        data = dummy_data_creator.CreateDummyOrg(municipality_code,
                                                 name, name_path, root_name)

        data.create_org_func_tree(org_size=org_size)

        data.add_users_to_tree(scale,
                               multiple_employments=org_size == Size.Large)
        return data

    def create_classes(self):
        for facet, klasser in self.data.classes.items():
            for klasse in klasser:
                if isinstance(klasse, tuple):
                    identifier = klasse[0]
                    title = klasse[1]
                    scope = klasse[2]
                else:
                    identifier = klasse
                    title = klasse
                    scope = 'TEXT'

                self.importer.add_klasse(
                    identifier=identifier,
                    facet_type_ref=facet,
                    user_key=identifier,
                    title=title,
                    scope=scope
                )

    def create_it_systems(self):
        for it_system in self.data.it_systems:
            self.importer.new_itsystem(
                identifier=it_system,
                system_name=it_system
            )

    def create_ou(self, ou_node):
        date_from = datetime.strftime(self.data.global_start_date, '%Y-%m-%d')

        if ou_node.parent:
            parent = ou_node.parent.key
        else:
            parent = None

        self.importer.add_organisation_unit(
            identifier=ou_node.key,
            name=ou_node.name,
            parent_ref=parent,
            type_ref='Afdeling',
            date_from=date_from
        )

        self.importer.add_address_type(
            organisation_unit=ou_node.key,
            value=ou_node.address['dar-uuid'],
            type_ref='AddressMailUnit',
            date_from=date_from
        )

        self.importer.add_address_type(
            organisation_unit=ou_node.key,
            value=ou_node.place_of_contact['dar-uuid'],
            type_ref='AdresseHenvendelsessted',
            date_from=date_from
        )

        self.importer.add_address_type(
            organisation_unit=ou_node.key,
            value=ou_node.address['dar-uuid'],
            type_ref='AdressePostRetur',
            date_from=date_from
        )

        self.importer.add_address_type(
            organisation_unit=ou_node.key,
            value=ou_node.email,
            type_ref='EmailUnit',
            date_from=date_from
        )
        self.importer.add_address_type(
            organisation_unit=ou_node.key,
            value=ou_node.ean,
            type_ref='EAN',
            date_from=date_from
        )

        self.importer.add_address_type(
            organisation_unit=ou_node.key,
            value=ou_node.p_number,
            type_ref='p-nummer',
            date_from=date_from
        )

        if ou_node.location:
            self.importer.add_address_type(
                organisation_unit=ou_node.key,
                value=ou_node.location,
                type_ref='LocationUnit',
                date_from=date_from
            )

        if ou_node.url:
            self.importer.add_address_type(
                organisation_unit=ou_node.key,
                value=ou_node.url,
                type_ref='WebUnit',
                date_from=date_from
            )

        """
        # IT-systems for OUs are currently not supported by importer
        for it_system in self.data.it_systemer:
            self.org.OrganisationUnit.add_type_itsystem(
                owner_ref=owner_ref,
                user_key=owner_ref,
                itsystem_ref=it_system,
                date_from='2010-02-01'
            )
        """

    def create_user(self, user_node):
        self.importer.add_employee(
            name=user_node.name,
            user_key=user_node.user[0]['brugervendtnoegle'],
            identifier=user_node.user[0]['brugervendtnoegle'],
            cpr_no=user_node.user[0]['cpr']
        )

        for user in user_node.user:  # All user information is here
            date_from = datetime.strftime(user['fra'], '%Y-%m-%d')
            if user['til'] is not None:
                date_to = datetime.strftime(user['til'], '%Y-%m-%d')
            else:
                date_to = None
            owner_ref = user['brugervendtnoegle']

            self.importer.add_engagement(
                employee=owner_ref,
                organisation_unit=user_node.parent.key,
                job_function_ref=user['job_function'],
                engagement_type_ref="Ansat",
                date_from=date_from,
                date_to=date_to
            )

            self.importer.add_address_type(
                employee=owner_ref,
                value=user['address']['dar-uuid'],
                type_ref="AdressePostEmployee",
                date_from=date_from,
                date_to=date_to
            )

            self.importer.add_address_type(
                employee=owner_ref,
                value=user['phone'],
                visibility=user['secret_phone'],
                type_ref="PhoneEmployee",
                date_from=date_from,
                date_to=date_to
            )

            self.importer.add_address_type(
                employee=owner_ref,
                value=user['email'],
                type_ref="EmailEmployee",
                date_from=date_from,
                date_to=date_to
            )

            if user['location']:
                self.importer.add_address_type(
                    employee=owner_ref,
                    value=user['location'],
                    type_ref="LocationEmployee",
                    date_from=date_from,
                    date_to=date_to
                )

            for it_system in user['it_systemer']:
                self.importer.join_itsystem(
                    employee=owner_ref,
                    user_key=owner_ref,
                    itsystem_ref=it_system,
                    date_from=date_from,
                    date_to=date_to
                )

            if user['association'] is not None:
                association = user['association']

                self.importer.add_association(
                    employee=owner_ref,
                    organisation_unit=str(association['unit']),
                    association_type_ref=association['type'],
                    date_from=date_from,
                    date_to=date_to
                )

            if user['role'] is not None:
                role = user['role']
                self.importer.add_role(
                    employee=owner_ref,
                    organisation_unit=str(role['unit']),
                    role_type_ref=role['type'],
                    date_from=date_from,
                    date_to=date_to

                )

            if user['manager']:
                self.importer.add_manager(
                    employee=owner_ref,
                    organisation_unit=user_node.parent.key,
                    manager_type_ref="Direktør",  # TODO
                    manager_level_ref='Niveau 4',  # TODO
                    responsibility_list=user['manager'],
                    date_from=date_from,
                    date_to=date_to
                )


@command(help=__doc__, context_settings=dict(
    auto_envvar_prefix='MO',
    help_option_names=['-h', '--help'],
))
@option('--mox-base', metavar='URL', default='http://localhost:5000')
@option('--mora-base', metavar='URL', default='http://localhost')
@option('--municipality', metavar='NAME', default='København')
@option('--scale', type=int, default=4)
@option('--extra-root/--no-extra-root', is_flag=True)
@option('--small', 'org_size', flag_value=Size.Small)
@option('--normal', 'org_size', flag_value=Size.Normal, default=True)
@option('--large', 'org_size', flag_value=Size.Large)
def main(mox_base, mora_base, municipality, scale, org_size, extra_root):
    with Session() as sess:
        r = sess.get('http://dawa.aws.dk/kommuner', params={
            'navn': municipality,
            'struktur': 'flad',
        })

        if not r or not r.json():
            raise Abort('no such municipality: ' + name)

        municipality_number = r.json()[0]['kode']
        municipality_name = r.json()[0]['navn'] + ' Kommune'

    importer = ImportHelper(create_defaults=True,
                            mox_base=mox_base,
                            mora_base=mora_base,
                            system_name="Artificial import",
                            end_marker="STOP_DUMMY",
                            store_integration_data=True)
    creator = CreateDummyOrg(importer,
                             municipality_code=municipality_number,
                             name=municipality_name,
                             scale=scale,
                             org_size=org_size,
                             extra_root=True)

    importer.import_all()

if __name__ == '__main__':
    main()
