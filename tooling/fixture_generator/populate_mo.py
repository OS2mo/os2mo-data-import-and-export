import dummy_data_creator
from datetime import datetime
from anytree import PreOrderIter
from os2mo_data_import import Organisation, ImportUtility


class CreateDummyOrg(object):

    def __init__(self, municipality_code, name, scale=1, heavy_data_set=False):
        self.data = self.create_dummy_data(municipality_code, name, scale,
                                           heavy_data_set)

        self.org = Organisation(
            name=self.data.nodes['root'].name,
            user_key=self.data.nodes['root'].name,
            municipality_code=municipality_code,
            create_defaults=True
        )

        self.create_classes()
        self.create_it_systems()

        for node in PreOrderIter(self.data.nodes['root']):
            if node.type == 'ou':
                self.create_ou(node)
            if node.type == 'user':
                self.create_user(node)

    def create_dummy_data(self, municipality_code, name, scale,
                          heavy_data_set):
        name_path = dummy_data_creator._path_to_names()
        data = dummy_data_creator.CreateDummyOrg(municipality_code,
                                                 name, name_path)
        data.create_org_func_tree(too_many_units=heavy_data_set)
        data.add_users_to_tree(scale, multiple_employments=heavy_data_set)
        return data

    def create_classes(self):
        for facet, klasser in self.data.classes.items():
            for klasse in klasser:
                self.org.Klasse.add(
                    identifier=klasse,
                    facet_type_ref=facet,
                    user_key=klasse,
                    title=klasse
                )

    def create_it_systems(self):
        for it_system in self.data.it_systems:
            self.org.Itsystem.add(
                identifier=it_system,
                system_name=it_system
            )

    def create_ou(self, ou_node):
        date_from = datetime.strftime(self.data.global_start_date, '%Y-%m-%d')
        if ou_node.parent:
            parent = ou_node.parent.key
        else:
            parent = None

        self.org.OrganisationUnit.add(
            identifier=ou_node.key,
            name=ou_node.name,
            parent_ref=parent,
            org_unit_type_ref="Afdeling",  # TODO
            date_from=date_from
        )

        self.org.OrganisationUnit.add_type_address(
            owner_ref=ou_node.key,
            uuid=ou_node.adresse['dar-uuid'],
            address_type_ref="AdressePost",
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
        self.org.Employee.add(
            name=user_node.name,
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

            self.org.Employee.add_type_engagement(
                owner_ref=owner_ref,
                org_unit_ref=user_node.parent.key,
                job_function_ref=user['job_function'],
                engagement_type_ref="Ansat",
                date_from=date_from,
                date_to=date_to
            )

            self.org.Employee.add_type_address(
                owner_ref=owner_ref,
                uuid=user['adresse']['dar-uuid'],
                address_type_ref="AdressePost",
                date_from=date_from,
                date_to=date_to
            )

            self.org.Employee.add_type_address(
                owner_ref=owner_ref,
                value=user['telefon'],
                address_type_ref="Telefon",
                date_from=date_from,
                date_to=date_to
            )

            self.org.Employee.add_type_address(
                owner_ref=owner_ref,
                value=user['email'],
                address_type_ref="Email",
                date_from=date_from,
                date_to=date_to
            )

            for it_system in user['it_systemer']:
                self.org.Employee.add_type_itsystem(
                    owner_ref=owner_ref,
                    user_key=owner_ref,
                    itsystem_ref=it_system,
                    date_from=date_from,
                    date_to=date_to
                )

            if user['association'] is not None:
                data_list = self.org.Employee.get(owner_ref)['optional_data'][0]
                for data in data_list:
                    if data[0] == 'job_function':
                        job_function = data[1]

                association = user['association']
                self.org.Employee.add_type_association(
                    owner_ref=owner_ref,
                    org_unit_ref=str(association['unit']),
                    job_function_ref=job_function,
                    association_type_ref=association['type'],
                    date_from=date_from,
                    date_to=date_to
                )

            if user['role'] is not None:
                role = user['role']
                self.org.Employee.add_type_role(
                    owner_ref=owner_ref,
                    org_unit_ref=str(role['unit']),
                    role_type_ref=role['type'],
                    date_from=date_from,
                    date_to=date_to

                )

            if user['manager']:
                self.org.Employee.add_type_manager(
                    owner_ref=owner_ref,
                    org_unit_ref=user_node.parent.key,
                    manager_type_ref="Direktør",  # TODO
                    manager_level_ref='Niveau 4',  # TODO
                    responsibility_list=user['manager'],
                    date_from=date_from,
                    date_to=date_to
                )


if __name__ == '__main__':
    creator = CreateDummyOrg(370, 'Næstved', scale=8, heavy_data_set=False)
    dummy_import = ImportUtility(
        dry_run=False,
        mox_base='http://localhost:8080',
        mora_base='http://localhost:80'
    )
    dummy_import.import_all(creator.org)
