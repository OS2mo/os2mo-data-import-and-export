# -- coding: utf-8 --

import copy
import json
from uuid import uuid4
from requests import Session
from datetime import datetime, timedelta
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
                 system_name='Import', store_integration_data=False,
                 end_marker='Jørgen'):
        self.system_name = system_name
        self.end_marker = end_marker
        self.existing_uuids = []  # List of all uuids we know already lives in LoRa
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

    def _integration_data(self, resource, reference, payload={},
                          encode_integration=True):
        """
        Update the payload with integration data. Checks if an object with this
        integration data already exists. In this case the uuid of the exisiting
        object is put into the payload. If a supplied uuid is inconsistent with
        the uuid found from integration data, an exception is raised.

        :param resource:
        LoRa resource URL.

        :param referece:
        Unique label that will be stored in the integration data to identify the
        object on re-import.

        :param payload:
        The supplied payload will be updated with values for integration and uuid
        (if the integration data was found from an earlier import). For MO objects,
        payload will typically be pre-populated and will then be ready for import
        when returned. For MOX objects, the initial payload
        will typically be empty, and the returned values can be fed to the relevant
        adapter.

        :param encode_integration:
        If True, the integration data will be returned in json-encoded form.

        :return:
        The original payload updated with integration data and object uuid, if the
        object was already imported.
        """
        # TODO: We need to have a list of all objects with integration data to
        # be able to make a list of objects that has disappeared
        if self.store_integration_data:
            service = urljoin(self.mox_base, resource)
            # integration_data = {self.system_name: reference + self.system_name}
            integration_data = {self.system_name: str(reference) + self.end_marker}
            if resource.find('klasse') > 0:
                query = service + '?retskilde=%{}%'
            else:
                query = service + '?integrationsdata=%{}%'
            query = query.format(json.dumps(integration_data)[1:-1])

            # Call repr to ensure escaping consistent with the payload from request
            response = self.session.get(url=repr(query)[1:-1])
            response = response.json()['results'][0]

            if len(response) == 0:
                pass

            elif len(response) == 1:
                uuid = response[0]
                self.existing_uuids.append(uuid)
                if 'uuid' in payload:
                    assert(uuid == payload['uuid'])
                else:
                    payload['uuid'] = uuid

                # Get the entire integration data string, we need to be polite
                # towards the existing content:
                object_url = '{}/{}'.format(service, uuid)
                object_data = self.session.get(object_url)
                object_data = object_data.json()[uuid]
                attributter = object_data[0]['registreringer'][0]['attributter']
                for key in attributter.keys():
                    if key.find('egenskaber') > 0:
                        egenskaber = attributter[key][0]

                if resource.find('klasse') > 0:
                    integration_data = json.loads(egenskaber['retskilde'])
                else:
                    integration_data = json.loads(egenskaber['integrationsdata'])
            else:
                raise Exception('Inconsistent integration data!')

            if encode_integration:
                payload['integration_data'] = json.dumps(integration_data)
            else:
                payload['integration_data'] = integration_data
        return payload

    def _std_compare(self, item_payload, data_item, extra_field=None):
        """ Helper for _payload_compare, performs the checks that are identical
        for most object types.
        :param item_payload: The new payload data.
        :param data_item: The existing set of data.
        :param extra_field: If not None the comparison will also be done on this
        field, otherwise the comparison is only performed on uuid and validity.
        :return: True if identical, otherwise False
        """
        identical = (
            (data_item['org_unit']['uuid'] == item_payload['org_unit']['uuid']) and
            (data_item['validity']['from'] == item_payload['validity']['from']) and
            (data_item['validity']['to'] == item_payload['validity']['to'])
        )
        if extra_field is not None:
            identical = (
                identical and
                data_item[extra_field]['uuid'] == item_payload[extra_field]['uuid']
            )
        return identical

    def _payload_compare(self, item_payload, data):
        """ Compare an exising data-set with a new payload and tell whether
        the new payload is different from the exiting data.
        :param item_payload: New the payload data.
        :param data_item: The existing set of data.
        :param extra_field: If not None the comparison will also be done on this
        field, otherwise the comparison is only performed on uuid and validity.
        :return: True if identical, otherwise False
        """
        data_type = item_payload['type']
        found_hit = False
        if data_type == 'engagement':
            for data_item in data[data_type]:
                if self._std_compare(item_payload, data_item, 'job_function'):
                    found_hit = True

        elif data_type == 'role':
            for data_item in data[data_type]:
                if self._std_compare(item_payload, data_item, 'role_type'):
                    found_hit = True

        elif data_type == 'it':
            for data_item in data[data_type]:
                if (
                    (data_item['validity']['from'] ==
                     item_payload['validity']['from']) and

                    (data_item['validity']['to'] ==
                     item_payload['validity']['to']) and

                    (data_item['itsystem']['uuid'] ==
                     item_payload['itsystem']['uuid'])
                ):
                    found_hit = True

        elif data_type == 'address':
            for data_item in data[data_type]:
                if (
                    (data_item['validity']['from'] ==
                     item_payload['validity']['from']) and

                    (data_item['validity']['to'] ==
                     item_payload['validity']['to']) and

                    (data_item['href'] == item_payload['value'])
                ):
                    found_hit = True
            found_hit = True

        elif data_type == 'manager':
            for data_item in data[data_type]:
                identical = self._std_compare(item_payload, data_item,
                                              'manager_level')
                uuids = []
                for item in item_payload['responsibility']:
                    uuids.append(item['uuid'])
                for responsibility in data_item['responsibility']:
                    identical = identical and (responsibility['uuid'] in uuids)
                identical = (identical and
                             (len(data_item['responsibility']) == len(uuids)))
                if identical:
                    found_hit = True

        elif data_type == 'association':
            for data_item in data[data_type]:
                if self._std_compare(item_payload, data_item, 'association_type'):
                    found_hit = True
        else:
            raise Exception('Uknown detail!')
        return found_hit

    def insert_mox_data(self, resource, data, uuid=None):
        """
        Insert post data into the MOX/OIO REST interface

        :param resource:
        Resource path of the service endpoint (str) e.g. /organisation/organisation

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
        if uuid is not None:
            assert(uuid == response_data["uuid"])
        return response_data["uuid"]

    def insert_mora_data(self, resource, data):
        """
        Insert post data into the MORA backend

        :param resource:
        Resource path of the service endpoint (str) e.g. /service/ou/create

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

        organisation_uuid = integration_data.get('uuid', None)
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

        klassifikation_uuid = integration_data.get('uuid', None)
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

        facet_uuid = integration_data.get('uuid', None)
        uuid = self.insert_mox_data(resource=resource, data=payload, uuid=facet_uuid)

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

        uuid = klasse.get('uuid', None)
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

        payload = adapters.klasse_payload(
            klasse=klasse_data,
            facet_uuid=facet_uuid,
            organisation_uuid=self.organisation_uuid,
            validity=self.global_validity,
            integration_data=integration_data
        )

        if 'uuid' in integration_data:
            klasse_uuid = integration_data['uuid']
            assert(uuid is None or klasse_uuid == uuid)
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

        resource = 'organisation/itsystem'
        name = itsystem['itsystemnavn']
        integration_data = self._integration_data(resource, name, {})
        if 'uuid' in integration_data:
            itsystem_uuid = integration_data['uuid']
        else:
            itsystem_uuid = None

        payload = adapters.itsystem_payload(
            itsystem=itsystem,
            organisation_uuid=self.organisation_uuid,
            validity=self.global_validity,
            integration_data=integration_data
        )

        return self.insert_mox_data(
            resource=resource,
            data=payload,
            uuid=itsystem_uuid
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
                                         reference, payload,
                                         encode_integration=False)

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

    def _get_detail(self, uuid, field_type):
        """ Get information from /detail for an employee
        :param uuid: uuid for the employee
        :param field_type: detail field type
        :return: dict with the relevant information
        """
        all_data = []
        for validity in ['past', 'present', 'future']:
            service = urljoin(self.mora_base, 'service/e/{}/details/{}?validity={}')
            url = service.format(uuid, field_type, validity)
            data = self.session.get(url)
            data = data.json()
            all_data += data
        return all_data

    def _terminate_employee(self, uuid):
        service = urljoin(self.mora_base, 'service/e/{}/terminate')
        yesterday = datetime.now() - timedelta(days=1)
        payload = {'validity': {'to': yesterday.strftime('%Y-%m-%d')}}
        url = service.format(uuid)
        data = self.session.post(url, json=payload).json()
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
                                         reference, payload,
                                         encode_integration=False)

        # We unconditionally create or update the user, this should
        # ensure that we alwas updated with correct current information.
        uuid = self.insert_mora_data(resource="service/e/create", data=payload)
        if 'uuid' in payload:
            assert(uuid == payload['uuid'])

        # Add uuid to the inserted employee map
        self.inserted_employee_map[reference] = uuid

        data = {}
        data['it'] = self._get_detail(uuid, 'it')
        data['role'] = self._get_detail(uuid, 'role')
        # A bug in MO implies that the address information is thrown away after
        # the re-creation of the underlying user.
        # data['address'] = self._get_detail(uuid, 'address')
        data['manager'] = self._get_detail(uuid, 'manager')
        data['engagement'] = self._get_detail(uuid, 'engagement')
        data['association'] = self._get_detail(uuid, 'association')

        # Details: /service/details/create endpoint
        if optional_data:
            complete_additional_payload = []
            additional_payload = []
            for item in optional_data:
                found_hit = False
                item_payload = self.build_mo_payload(item, person_uuid=uuid)

                if item_payload['type'] in data.keys():
                    found_hit = self._payload_compare(item_payload, data)
                else:
                    # raise Exception('Unknown payload')
                    # When the address-bug is fixed, we can raise an exception here
                    pass

                new_item_payload = copy.deepcopy(item_payload)
                today = datetime.now().strftime('%Y-%m-%d')
                valid_to = new_item_payload['validity']['to']
                future = datetime.strptime(valid_to, '%Y-%m-%d') > datetime.now()
                if (valid_to is None) or (future is True):

                    new_item_payload['validity']['from'] = today
                    complete_additional_payload.append(new_item_payload)
                    # Clean this up. We do not need a long and a short list
                    # of payloads, we need to know if something changes and thus
                    # if we need to terminate and re-hire the employee
                    # if not found_hit. This awaits fixing the current issues in MO.
                additional_payload.append(item_payload)

            # Hvad sker der, hvis man fyrer en person og ansætter igen samme dag...?
            # This will always happen as long as the date-bug exists
            if uuid in self.existing_uuids and len(additional_payload) > 0:
                print('Terminate: {}'.format(uuid))
                self._terminate_employee(uuid)
                self.insert_mora_data(
                    resource="service/details/create",
                    data=complete_additional_payload
                )
            else:
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

        # Insert Organisation
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
