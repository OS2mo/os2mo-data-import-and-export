import dummy_data_creator
from anytree import PreOrderIter
from os2mo_data_import import Organisation, ImportUtility


def _create_klasser(org):
    org.Klasse.add(
        identifier="Udvikler",
        facet_type_ref="Stillingsbetegnelse",
        user_key="Udvikler",
        title="Udvikler"
    )

    org.Klasse.add(
        identifier="Afdeling",
        facet_type_ref="Enhedstype",
        user_key="91154D1E-E7CA-439B-B910-D4622FD3FD21",
        title="Afdeling"
    )

    # Manager type
    org.Klasse.add(
        identifier="Direktør",
        facet_type_ref="Ledertyper",
        user_key="Direktør",
        title="Virksomhedens direktør"
    )

    # Manager level
    org.Klasse.add(
        identifier="Højeste niveau",
        facet_type_ref="Lederniveau",
        user_key="Højeste niveau",
        title="Højeste niveau"
    )

    # Add responsabilities
    org.Klasse.add(
        identifier="Tage beslutninger",
        facet_type_ref="Lederansvar",
        user_key="Tage beslutninger",
        title="Tage beslutninger"
    )


def create_dummy_data(kommunekode, navn):
    name_path = dummy_data_creator._path_to_names()
    dummy_creator = dummy_data_creator.CreateDummyOrg(kommunekode,
                                                      navn,
                                                      name_path)
    dummy_creator.create_org_func_tree()
    dummy_creator.add_users_to_tree(ou_size_scale=1)
    return dummy_creator


def _create_user(org, user_node):
            user = user_node.user  # All unser information is here

            org.Employee.add(
                name=user_node.name,
                identifier=user['brugervendtnoegle'],
                cpr_no=user['cpr']
            )

            org.Employee.add_type_engagement(
                owner_ref=user['brugervendtnoegle'],
                org_unit_ref=user_node.parent.key,
                job_function_ref="Projektleder",  # TODO
                engagement_type_ref="Ansat",
                date_from="1986-02-01"
            )

            org.Employee.add_type_address(
                owner_ref=user['brugervendtnoegle'],
                uuid=user['adresse']['dar-uuid'],
                address_type_ref="AdressePost",
                date_from="1986-02-01",
            )

            org.Employee.add_type_address(
                owner_ref=user['brugervendtnoegle'],
                value=user['telefon'],
                address_type_ref="Telefon",
                date_from=user['fra'],
                # date_to=user['to']  # No date_to keyword?
            )

            # E-mail

            if user['manager']:
                org.Employee.add_type_manager(
                    owner_ref=user['brugervendtnoegle'],
                    org_unit_ref=user_node.parent.key,
                    manager_type_ref="Direktør",
                    manager_level_ref="Højeste niveau",
                    responsibility_list=["Tage beslutninger",
                                         "Motivere medarbejdere",
                                         "Betale løn"],
                    date_from="1986-12-01",
                )


def create_dummy_org(dummy_data):
    org = Organisation(
        name=dummy_data.nodes['root'].name,
        user_key=dummy_data.nodes['root'].name,
        municipality_code=dummy_data.kommunekode,
        create_defaults=True
    )

    _create_klasser(org)

    for node in PreOrderIter(dummy_data.nodes['root']):
        if node.type == 'ou':
            if node.parent:
                parent = node.parent.key
            else:
                parent = None

            org.OrganisationUnit.add(
                identifier=node.key,
                name=node.name,
                parent_ref=parent,
                org_unit_type_ref="Afdeling",  # TODO
                date_from="1986-01-01"  # TODO
            )

            org.OrganisationUnit.add_type_address(
                owner_ref=node.key,
                uuid=node.adresse['dar-uuid'],
                address_type_ref="AdressePost",
                date_from="1986-01-01"
            )

        if node.type == 'user':
            _create_user(org, node)
    return org


if __name__ == '__main__':
    dummy_data = create_dummy_data(860, 'Hjørring Kommune')
    org = create_dummy_org(dummy_data)

    dummy_import = ImportUtility(
        dry_run=True,
        mox_base='http://localhost:8080',
        mora_base='http://localhost:80'
    )
    dummy_import.import_all(org)
