# -- coding: utf-8 --

import json
from uuid import uuid4
from requests import Session
from urllib.parse import urljoin

import os2mo_data_import.adapters as adapters
from os2mo_data_import import Organisation

# Default settings
MOX_BASE = "http://localhost:8080"
MORA_BASE = "http://localhost:5000"


class ImportUtility(object):
    """
        The ImportUtility class is the handler for storing
        the organisation content into the os2mo datastore.

        :param dry_run:
            A toggle for a simulation of the import procedure (bool)
            During a dry run, uuid's for inserts are generated
            and the post data payloads are shown in json format.

        :param mox_base:
            The base url of the mox backend (str)
            E.g. http://mox.magenta.dk

        :param mora_base:
            The base url of the mora backend (str)
            E.g. http://mora.magenta.dk

        """

    def __init__(self, dry_run=False, mox_base=MOX_BASE, mora_base=MORA_BASE,
                 system_name='Import', store_integration_data=False):
        self.system_name = system_name
        self.store_integration_data = store_integration_data

        # Service endpoints
        self.mox_base = mox_base
        self.mora_base = mora_base

        # Session
        self.dry_run = dry_run
        self.session = Session()

        # Inserted uuid maps
        self.inserted_facet_map = {}
        self.inserted_klasse_map = {}
        self.inserted_org_unit_map = {}
        self.inserted_employee_map = {}
        self.inserted_itsystem_map = {}

    def _get_mo_organisation(self):
        """
        Return the name and uuid of an existing organisation
        :return: None if no organisation exisits, otherwise a dict with name and uuid
        """

        resource = 'service/o/'
        service = urljoin(self.mora_base, resource)
        response = self.session.get(url=service)
        response = response.json()
        if len(response) == 0:
            return_val = None
        elif len(response) == 1:
            return_val = response[0]
        else:
            # In principle, we could support more organisations, but currently
            # this is an uneeded complexity
            raise Exception('Too many organisation')
        return return_val

    # TODO: This can be removed!!!!!
    def _get_mox_object(self, name, resource, relation=None):
        """
        Find an existing facet in LoRa. This really should be done using integration
        data, but since this is currently not available, it is implemented as a
        name search.
        :return: None if the facet does not exist otherwise, return the uuid
        """
        print('Name: {}, resource: {}, relation: {}'.format(name, resource,
                                                            relation))
        if relation is None:
            resource = resource.format(name['brugervendtnoegle'])
        else:
            resource = resource.format(name['brugervendtnoegle'], relation)
        service = urljoin(self.mox_base, resource)
        response = self.session.get(url=service)
        response = response.json()['results'][0]
        if len(response) == 0:
            return_val = None
        elif len(response) == 1:
            return_val = response[0]
        else:
            print(service)
            raise Exception('Too many objects with name {}'.format(name))
        return return_val

    def _integration_data(self, resource, reference, payload={}):
        """
        Update the payload with integration data. Checks if an object with this
        integration data already exists. In this case the uuid of the exisiting
        object is put into the payload. If a supplied uuid is inconsistent with
        the uuid found from integration data, an exception is raised.
        :param resource: TODO
        :param payload: TODO
        :param referece: TODO
        :return: TODO
        """
        if self.store_integration_data:
            service = urljoin(self.mox_base, resource)
            integration_data = json.dumps({self.system_name: reference})

            if resource.find('klasse') > 0:
                query = service + '?retskilde=%{}%'.format(integration_data)
            else:
                query = service + '?integrationsdata=%{}%'.format(integration_data)
            response = self.session.get(url=query)
            response = response.json()['results'][0]

            if len(response) == 0:
                pass
            elif len(response) == 1:
                uuid = response[0]
                print(payload)
                if 'uuid' in payload:
                    assert(uuid == payload['uuid'])
                else:
                    payload['uuid'] = uuid
            else:
                raise Exception('Inconsistent integration data!')

            payload['integration_data'] = integration_data
        return payload

    def insert_mox_data(self, resource, data, uuid=None):
        """
        Insert post data into the MOX/OIO REST interface

        :param resource:
            Resource path of the service endpoint (str)
            e.g. /organisation/organisation

        :param data:
            Post data object (dict)
            Metadata converted into OIO REST formatted post data

        :return:
            Inserted UUID (str)

        """

        service = urljoin(self.mox_base, resource)

        if self.dry_run:
            print(
                json.dumps(data, indent=2)
            )
            if uuid is None:
                dry_uuid = uuid4()
            else:
                dry_uuid = uuid
            response_data = {
                "uuid": str(
                    dry_uuid
                )
            }
        else:
            if uuid is not None:
                response = self.session.put(url=service + '/' + uuid, json=data)
            else:
                response = self.session.post(url=service, json=data)
            response_data = response.json()
        return response_data["uuid"]

    def insert_mora_data(self, resource, data):
        """
        Insert post data into the MORA backend

        :param resource:
            Resource path of the service endpoint (str)
            e.g. /service/ou/create

        :param data:
            Post data object (dict)
            Metadata converted into OIO REST formatted post data

        :return:
            Inserted UUID (str)

        """
        service = urljoin(self.mora_base, resource)

        if self.dry_run:

            print(
                json.dumps(data, indent=2)
            )
            if 'uuid' in data:
                dry_uuid = data['uuid']
            else:
                dry_uuid = uuid4()
            response_data = str(
                dry_uuid
            )
        else:
            response = self.session.post(url=service, json=data)
            response_data = response.json()

        return response_data

    def get_facet_types(self):
        """
        Retrieve a list of klasse type items
        These are needed to create the correct post data payloads
        for address type objects for organisation units and employees.

        For more detailed information, please refer to the official mora docs:
        https://mora.readthedocs.io/en/development/api/address.html

        """

        if hasattr(self, "facet_types"):
            return

        self.facet_types = {}

        resource = "service/o/{uuid}/f/{type}/".format(
            uuid=self.organisation_uuid,
            type="address_type"
        )

        service = urljoin(self.mora_base, resource)

        if self.dry_run:
            for value in self.inserted_klasse_map.values():
                self.facet_types[value] = {
                    "uuid": value
                }
        else:
            response = self.session.get(service)

            if response.status_code != 200:
                print(response.text)
                raise RuntimeError(response.text)

            response_data = response.json()

            if "items" not in response_data["data"]:
                return False

            for item in response_data["data"]["items"]:
                uuid = item["uuid"]
                self.facet_types[uuid] = item

        return True

    def import_organisation(self, org_export):
        """
        Convert organisation to OIO formatted post data
        and import into the MOX datastore.

        :param org_export:
            Data objected returned by the export() method (dict)

        :returns:
            Inserted UUID (str)

        """
        name = org_export['data']['organisationsnavn']
        resource = "organisation/organisation"
        integration_data = self._integration_data(resource, name, {})

        payload = adapters.organisation_payload(
            organisation=org_export["data"],
            municipality_code=org_export["municipality_code"],
            validity=self.global_validity,
            integration_data=integration_data
        )

        # The uuid should not be an actual part of the payload, we pick it out
        # and serve it to insert_mox_data instead
        if 'uuid' in payload:
            organisation_uuid = payload['uuid']
            del(payload['uuid'])
        else:
            organisation_uuid = None

        self.organisation_uuid = self.insert_mox_data(
            resource="organisation/organisation",
            data=payload,
            uuid=organisation_uuid
        )

        return self.organisation_uuid

    def import_klassifikation(self, parent_name):
        """
        Generate and insert a klassifikation object
        This is the parent of all the facet types which
        belong to the organisation.

        :param parent_name:
            The user_key of the parent organisation (str)
            This is used to generate the user_key, description
            and alias for the klassifikation object.

        :returns:
            Inserted UUID (str)

        """

        user_key = "Organisation {name}".format(name=parent_name)
        description = "Belongs to {name}".format(name=parent_name)

        resource = "klassifikation/klassifikation"
        integration_data = self._integration_data(resource, user_key, {})

        klassifikation = {
            "brugervendtnoegle": user_key,
            "beskrivelse": description,
            "kaldenavn": parent_name,
        }

        payload = adapters.klassifikation_payload(
            klassifikation=klassifikation,
            organisation_uuid=self.organisation_uuid,
            validity=self.global_validity,
            integration_data=integration_data
        )

        # The uuid should not be an actual part of the payload, we pick it out
        # and serve it to insert_mox_data instead
        if 'uuid' in payload:
            klassifikation_uuid = payload['uuid']
            del(payload['uuid'])
        else:
            klassifikation_uuid = None

        self.klassifikation_uuid = self.insert_mox_data(
            resource="klassifikation/klassifikation",
            data=payload,
            uuid=klassifikation_uuid
        )

        return self.klassifikation_uuid

    def import_facet(self, reference, facet):
        """
        Generate and insert a facet object
        This is the parent of all the klasse type objects.

        :param reference:
            Reference to the user defined identifier (str)

        :param klasse:
            Facet type data object (dict)

        :returns:
            Inserted UUID (str)

        """

        resource = "klassifikation/facet"
        integration_data = self._integration_data(resource, reference, {})
        payload = adapters.facet_payload(
            facet=facet,
            klassifikation_uuid=self.klassifikation_uuid,
            organisation_uuid=self.organisation_uuid,
            validity=self.global_validity,
            integration_data=integration_data
        )

        # The uuid should not be an actual part of the payload, we pick it out
        # and serve it to insert_mox_data instead
        if 'uuid' in payload:
            facet_uuid = payload['uuid']
            del(payload['uuid'])
        else:
            facet_uuid = None

        uuid = self.insert_mox_data(
            resource=resource,
            data=payload,
            uuid=facet_uuid
        )
        self.inserted_facet_map[reference] = uuid
        return uuid

    def import_klasse(self, reference, klasse):
        """
        Insert a klasse object

        :param reference:
            Reference to the user defined identifier (str)

        :param klasse:
            Klasse type data object (dict)

        :returns:
            Inserted UUID (str)

        """
        uuid = klasse['uuid']
        klasse_data = klasse["data"]
        facet_type_ref = klasse["facet_type_ref"]

        facet_uuid = self.inserted_facet_map.get(facet_type_ref)

        if not facet_uuid:
            error_message = "Facet ref: {ref} does not exist".format(
                ref=facet_type_ref
            )
            raise KeyError(error_message)

        resource = "klassifikation/klasse"
        integration_data = self._integration_data(resource, reference, {})

        # mox_klasse = self._get_mox_object(klasse['data'],
        #                                  resource,
        #                                  relation=uuid)

        # If mox_klasse contains an uuid, the Klasse was inserted in a
        # previous import.
        payload = adapters.klasse_payload(
            klasse=klasse_data,
            facet_uuid=facet_uuid,
            organisation_uuid=self.organisation_uuid,
            validity=self.global_validity,
            integration_data=integration_data
        )

        # The uuid should not be an actual part of the payload, we pick it out
        # and serve it to insert_mox_data instead
        if 'uuid' in payload:
            klasse_uuid = payload['uuid']
            assert(uuid is None or klasse_uuid == uuid)
            del(payload['uuid'])
        else:
            klasse_uuid = uuid

        import_uuid = self.insert_mox_data(
            resource="klassifikation/klasse",
            data=payload,
            uuid=klasse_uuid
        )
        assert(uuid is None or import_uuid == uuid)

        self.inserted_klasse_map[reference] = import_uuid

        return import_uuid

    def import_itsystem(self, itsystem):
        """
        Insert an itsystem object

        :param itsystem:
            Itsystem data object (dict)

        :returns:
            Inserted UUID (str)

        """

        payload = adapters.itsystem_payload(
            itsystem=itsystem,
            organisation_uuid=self.organisation_uuid,
            validity=self.global_validity
        )

        return self.insert_mox_data(
            resource="organisation/itsystem",
            data=payload
        )

    def import_org_unit(self, reference, organisation_unit_data, optional_data=None):
        """
        Insert primary and optional data for an organisation unit

        Optional data objects are relational objects which
        belong to the organisation unit, such as an address type

        :param reference:
            Reference to the user defined identifier (str)

        :param organisation_unit_data:
            Organisation Unit primary data object (dict)

        :param optional_data:
            Organisation Unit optional data object (dict)

        :returns:
            Inserted UUID (str)

        """
        print('Import org unit')
        if reference in self.inserted_org_unit_map:
            print("The organisation unit has already been inserted")
            return False

        payload = self.build_mo_payload(organisation_unit_data)
        payload = self._integration_data('organisation/organisationenhed',
                                         reference, payload)

        if optional_data:
            additional_payload = [
                self.build_mo_payload(item)
                for item in optional_data
            ]

            addresses = {
                "addresses": additional_payload
            }

            payload.update(addresses)

        uuid = self.insert_mora_data(
            resource="service/ou/create",
            data=payload
        )
        if 'uuid' in payload:
            assert(uuid == payload['uuid'])
        if not uuid:
            raise ConnectionError("Something went wrong")

        # Add to the inserted map
        self.inserted_org_unit_map[reference] = uuid

        return uuid

    def import_employee(self, reference, employee_data, optional_data=None):
        """
        Insert primary and optional data for an employee

        Optional data objects are relational objects which
        belong to the employee, such as an engagement, address, role etc.

        :param reference:
            Reference to the user defined identifier (str)

        :param employee_data:
            Employee primary data object (dict)

        :param optional_data:
            Employee optional data object (dict)

        :returns:
            Inserted UUID (str)

        """

        if reference in self.inserted_employee_map:
            print("Employee has already been inserted")
            return False

        payload = self.build_mo_payload(employee_data)
        payload = self._integration_data('organisation/bruger',
                                         reference, payload)

        uuid = self.insert_mora_data(resource="service/e/create", data=payload)
        if 'uuid' in payload:
            assert(uuid == payload['uuid'])

        # Add uuid to the inserted employee map
        self.inserted_employee_map[reference] = uuid

        # Details: /service/details/create endpoint
        if optional_data:
            additional_payload = [
                self.build_mo_payload(item, person_uuid=uuid)
                for item in optional_data
            ]

            self.insert_mora_data(
                resource="service/details/create",
                data=additional_payload
            )

        return uuid

    def build_mo_payload(self, list_of_tuples, person_uuid=None):
        """
        MORA backed post data builder
        A generic adapter for bulding json (dict) post data
        from a list of key value pairs.

        TODO:
            * This adapter is crude and needs to be reworked

        :param list_of_tuples:
            Accepts a list of tuples exported by
            the Organisation Unit and Employee classes.
            Example: (employee)

            [
                ("name", name),
                ("cpr_no", cpr_no),
                ("org", None)
            ]

        :param person_uuid:
            The UUID of the employee which optional data belongs to.
            If the parameter is passed, a reference to the person is
            attached to the final payload, e.g.

            {
                "person": {
                    "uuid": "A9E559BD-BA31-48CC-8898-E36A7FAF3E05"
                }
            }

        :return:
            Post data payload (dict)

        """

        payload = {}

        # Get facet types
        self.get_facet_types()

        # Prep for adapter
        build_value = None

        for key, val in list_of_tuples:

            if key in [
                "type",
                "name",
                "cpr_no",
                "validity",
                "uuid",
                "value",
                "user_key"
            ]:
                build_value = val

            if key in [
                "role_type",
                "leave_type",
                "it_type",
                "job_function",
                "engagement_type",
                "manager_type",
                "manager_level",
                "association_type"
            ]:
                uuid = self.inserted_klasse_map.get(val)
                build_value = {
                    "uuid": str(uuid)
                }

            if key == "itsystem":
                uuid = self.inserted_itsystem_map.get(val)
                build_value = {
                    "uuid": uuid
                }

            if key == "org":
                build_value = {
                    "uuid": self.organisation_uuid
                }

            if key == "org_unit":
                uuid = self.inserted_org_unit_map.get(val)
                build_value = {
                    "uuid": str(uuid)
                }

            if key == "address_type":
                uuid = self.inserted_klasse_map.get(val)
                address_type = self.facet_types.get(uuid)
                build_value = address_type

            if key == "address":
                type_uuid = self.inserted_klasse_map.get("AdressePost")
                address_type = self.facet_types.get(type_uuid)
                build_value = {
                    "uuid": str(val),
                    "address_type": address_type
                }

            if key == "parent":
                parent_uuid = self.inserted_org_unit_map.get(val)

                if not parent_uuid:
                    parent_uuid = self.organisation_uuid

                build_value = {
                    "uuid": str(parent_uuid)
                }

            if key == "org_unit_type":
                org_unit_type = self.inserted_klasse_map.get(val)

                if not org_unit_type:
                    raise ValueError("Type not found")

                build_value = {
                    "uuid": org_unit_type
                }

            if key == "responsibility":
                responsibility_list = []

                for responsibility in val:

                    uuid = self.inserted_klasse_map.get(responsibility)

                    reference = {
                        "uuid": uuid
                    }

                    responsibility_list.append(reference)

                build_value = responsibility_list

            if not build_value:
                continue

            payload[key] = build_value

        if person_uuid:
            payload["person"] = {
                "uuid": person_uuid
            }

        return payload

    def _import_org_units(self, identifier, org_unit):
        """
        HOTFIX: temporary fix for nested organisation units
        """
        parent_ref = org_unit["parent_ref"]

        # Insert parent if the organisation unit has a parent
        if parent_ref and parent_ref not in self.inserted_org_unit_map:

            parent_data = self.org.OrganisationUnit.get(parent_ref)
            self._import_org_units(parent_ref, parent_data)

        # Insert the actual organisation unit
        uuid = self.import_org_unit(
            reference=identifier,
            organisation_unit_data=org_unit["data"],
            optional_data=org_unit["optional_data"]
        )

        print("Inserted org unit: %s" % uuid)

    def import_all(self, org):
        """
        The main import function

        :param org:
            An object of the Organistion class type (Organisation)

        :return:
            A dummy return status (bool)

        """

        if not isinstance(org, Organisation):
            raise AssertionError("Object is not an instance of Organisation")

        # Set global validity
        self.global_validity = org.validity

        # HOTFIX: temporary fix for nested organisation units
        self.org = org

        # TODO: Remove this primitive check, now we use real integration data
        # Check if import has already been run on this organisation
        # mo_organisation = self._get_mo_organisation()
        # print(mo_organisation)
        # 1/0
        # if mo_organisation is not None:
        #    assert(mo_organisation['name'] == self.org.name)
        #    org_uuid = mo_organisation['uuid']
        #    self.organisation_uuid = org_uuid
        #    print("Found existig organisation: %s" % org_uuid)
        # else:
        #    # Insert Organisation
        org_export = org.export()
        org_uuid = self.import_organisation(org_export)
        print("Inserted organisation: %s" % org_uuid)

        # Insert Klassifikation
        parent_name = (org.user_key or org.name)
        klassifikation_uuid = self.import_klassifikation(parent_name)
        print("Inserted klassifikation: %s" % klassifikation_uuid)

        # Insert Facet
        for identifier, facet in org.Facet.export():
            uuid = self.import_facet(identifier, facet)
            print("Inserted facet: %s" % uuid)

        # Insert Klasse
        for identifier, klasse in org.Klasse.export():
            uuid = self.import_klasse(identifier, klasse)
            print("Inserted klasse: %s" % uuid)

        # Insert Itsystem
        print('Will now import IT-systems')
        for identifier, itsystem in org.Itsystem.export():
            uuid = self.import_itsystem(itsystem)
            self.inserted_itsystem_map[identifier] = uuid
            print("Inserted itsystem: %s" % uuid)

        # Insert Organisation Units
        print('Will now import org units')
        for identifier, org_unit in org.OrganisationUnit.export():
            self._import_org_units(identifier, org_unit)

        # Insert Employees
        print('Will now import employees')
        for identifier, employee in org.Employee.export():

            uuid = self.import_employee(
                reference=identifier,
                employee_data=employee["data"],
                optional_data=employee["optional_data"]
            )

            print("Inserted employee: %s" % uuid)

        return True
