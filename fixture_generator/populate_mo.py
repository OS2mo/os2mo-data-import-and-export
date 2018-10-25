import dummy_data_creator
from anytree import PreOrderIter
from os2mo_data_import import Organisation, ImportUtility


class CreateDummyOrg(object):

    def __init__(self, kommunekode, name, scale=1):
        # Fill in self.data
        self.create_dummy_data(kommunekode, name, scale=scale)

        self.org = Organisation(
            name=self.data.nodes['root'].name,
            user_key=self.data.nodes['root'].name,
            municipality_code=self.data.kommunekode,
            create_defaults=True
        )
        self.create_klasser()
        for node in PreOrderIter(self.data.nodes['root']):
            if node.type == 'ou':
                self.create_ou(node)
            if node.type == 'user':
                self.create_user(node)

    def create_dummy_data(self, kommunekode, navn, scale):
        name_path = dummy_data_creator._path_to_names()
        self.data = dummy_data_creator.CreateDummyOrg(kommunekode,
                                                      navn,
                                                      name_path)
        self.data.create_org_func_tree()
        self.data.add_users_to_tree(ou_size_scale=scale)
        return len(self.data.nodes)

    def create_klasser(self):
        for facet, klasser in self.data.klasser.items():
            for klasse in klasser:
                self.org.Klasse.add(
                    identifier=klasse,
                    facet_type_ref=facet,
                    user_key=klasse,
                    title=klasse
                )

    def create_ou(self, ou_node):
        if ou_node.parent:
            parent = ou_node.parent.key
        else:
            parent = None

        self.org.OrganisationUnit.add(
            identifier=ou_node.key,
            name=ou_node.name,
            parent_ref=parent,
            org_unit_type_ref="Afdeling",  # TODO
            date_from="1986-01-01"  # TODO
        )

        self.org.OrganisationUnit.add_type_address(
            owner_ref=ou_node.key,
            uuid=ou_node.adresse['dar-uuid'],
            address_type_ref="AdressePost",
            date_from="1986-01-01"
        )

    def create_user(self, user_node):
        user = user_node.user  # All user information is here

        self.org.Employee.add(
            name=user_node.name,
            identifier=user['brugervendtnoegle'],
            cpr_no=user['cpr']
        )

        self.org.Employee.add_type_engagement(
            owner_ref=user['brugervendtnoegle'],
            org_unit_ref=user_node.parent.key,
            job_function_ref='Udvikler',  # TODO
            engagement_type_ref="Ansat",
            date_from="2010-02-01"
        )

        self.org.Employee.add_type_address(
            owner_ref=user['brugervendtnoegle'],
            uuid=user['adresse']['dar-uuid'],
            address_type_ref="AdressePost",
            date_from="2010-02-01",
        )

        self.org.Employee.add_type_address(
            owner_ref=user['brugervendtnoegle'],
            value=user['telefon'],
            address_type_ref="Telefon",
            date_from=user['fra'],
            # date_to=user['to']  # No date_to keyword?
        )

        # E-mail

        if user['manager']:
            self.org.Employee.add_type_manager(
                owner_ref=user['brugervendtnoegle'],
                org_unit_ref=user_node.parent.key,
                manager_type_ref="Direktør",  # TODO
                manager_level_ref="Højeste niveau",  # TODO
                responsibility_list=user['manager'],
                date_from="2010-12-01",
            )


if __name__ == '__main__':
    creator = CreateDummyOrg(860, 'Fiktiv Hjørring 3', scale=3)
    dummy_import = ImportUtility(
        dry_run=False,
        mox_base='http://localhost:8080',
        mora_base='http://localhost:80'
    )
    dummy_import.import_all(creator.org)
