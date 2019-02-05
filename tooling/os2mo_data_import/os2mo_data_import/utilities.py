# -- coding: utf-8 --

from os2mo_data_import.mora_data_types import *
from os2mo_data_import.mox_data_types import *
from os2mo_data_import.defaults import facet_defaults, klasse_defaults

import json
from urllib.parse import urljoin
from requests import Session

# Default settings
MOX_BASE = "http://localhost:8080"
MORA_BASE = "http://localhost:5000"


class HttpUtility(Session):

    def __init__(self):
        super().__init__()

class TestHttpUtility(object):
    def __init__(self):
        pass



class DataStore(object):

    def __init__(self, dry_run=False, mox_base=MOX_BASE, mora_base=MORA_BASE,
                 system_name='Import', store_integration_data=False,
                 end_marker='Jørgen'):

        # Params
        self.dry_run = dry_run
        self.mox_base = mox_base
        self.mora_base = mora_base
        self.system_name = system_name
        self.store_integration_data = store_integration_data
        self.end_marker = end_marker

        # Session
        self.session = HttpUtility()

        # Placeholder for UUID import
        self.organisation_uuid = None

        # Existing UUIDS
        # TODO: More elegant version of this please
        self.existing_uuids = []

        # UUID map
        self.inserted_organisation = {}

    def insert_organisation(self, identifier, organisation):

        self.insert_mox_data("organisation", identifier, organisation)

    def import_organisation(self, organisation):
        """
        Convert organisation to OIO formatted post data
        and import into the MOX datastore.

        :param org_export:
        Data objected returned by the export() method (dict)

        :returns:
        Inserted UUID (str)
        """

        if not isinstance(organisation, Organisation):
            raise TypeError("Not of type Organisation")

        name = organisation.name
        resource = "organisation/organisation"

        integration_data = self._integration_data(
            resource=resource,
            reference=name,
            payload={}
        )

        # Set integration data
        # if integration_data:
        #     organisation.integration_data = integration_data

        payload = organisation.build()

        organisation_uuid = integration_data.get('uuid', None)

        self.organisation_uuid = self.insert_mox_data(
            resource=resource,
            data=payload,
            uuid=organisation_uuid
        )

        return self.organisation_uuid

    def insert_facet(self):
        pass

    def insert_klasse(self):
        pass

    def insert_mox_data(self, resource, data, uuid=None):

        print(data)
        print(resource)

        return uuid



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
            query_params = {
                "integrationsdata": "%{}%"
            }

            # integration_data = {self.system_name: reference + self.system_name}

            integration_data = {
                self.system_name: "{reference}{end}".format(
                    reference=reference,
                    end=self.end_marker
                )
            }

            integration_data_as_json = json.dumps(integration_data)[1:-1]

            # Call repr to ensure escaping consistent with the payload from request
            response = self.session.get(url=service, params=query_params)
            response = response.json()['results'][0]

            if len(response) == 0:
                pass

            elif len(response) == 1:
                uuid = response[0]
                self.existing_uuids.append(uuid)
                if 'uuid' in payload:
                    assert (uuid == payload['uuid'])
                else:
                    payload['uuid'] = uuid

                # Get the entire integration data string, we need to be polite
                # towards the existing content:

                # Scope issue
                egenskaber = None

                object_url = '{}/{}'.format(service, uuid)
                object_data = self.session.get(object_url)
                object_data = object_data.json()[uuid]
                attributter = object_data[0]['registreringer'][0]['attributter']
                for key in attributter.keys():
                    if key.find('egenskaber') > 0:
                        egenskaber = attributter[key][0]

                integration_data = json.loads(egenskaber['integrationsdata'])

            else:
                raise ValueError('Inconsistent integration data!')

            if encode_integration:
                payload['integration_data'] = json.dumps(integration_data)
            else:
                payload['integration_data'] = integration_data

        return payload


class ImportUtility(object):

    def __init__(self, create_defaults=True):

        self.organisation = {}
        self.klassifikation = {}

        self.klasse_objects = {}
        self.facet_objects = {}
        self.addresses = []
        self.itsystems = {}

        self.organisation_units = {}
        self.employees = {}

        # Create default facet and klasse
        if create_defaults:
            self.create_default_facet_types()
            self.create_default_klasse_types()

    def create_validity(self, date_from, date_to):

        if not date_from or not date_to:
            raise AssertionError("Date is not specified, cannot create validity")

        return {
            "from": date_from,
            "to": date_to
        }

    def add_organisation(self, identifier, **kwargs):

        name = (
            kwargs.get("name") or identifier
        )

        self.organisation[identifier] = Organisation(name=name, **kwargs)
        self.klassifikation[identifier] = Klassifikation(user_key=name, parent_name=name, description="umbrella")

    def add_klasse(self, identifier, **kwargs):

        if identifier in self.klasse_objects:
            raise ReferenceError("Unique constraint - Klasse identifier exists")

        if "user_key" not in kwargs:
            kwargs["user_key"] = identifier

        self.klasse_objects[identifier] = Klasse(**kwargs)

    def add_facet(self, identifier, **kwargs):

        if identifier in self.facet_objects:
            raise ReferenceError("Unique constraint - Facet identifier exists")

        self.facet_objects[identifier] = Facet(**kwargs)


    def add_organisation_unit(self, identifier, **kwargs):

        if identifier in self.organisation_units:
            raise ReferenceError("Identifier exists")

        self.organisation_units[identifier] = OrganisationUnitType(**kwargs)

    def add_employee(self, identifier, **kwargs):

        if identifier in self.employees:
            raise ReferenceError("Identifier exists")

        if "name" not in kwargs:
            kwargs["name"] = identifier

        self.employees[identifier] = EmployeeType(**kwargs)


    def add_address_type(self, organisation_unit=None, employee=None, **kwargs):

        if not (organisation_unit or employee):
            raise ReferenceError("Either organisation unit or employee must be owner")

        if organisation_unit and employee:
            raise ReferenceError("Must reference either organisation unit or employee and not both")

        if employee:

            if employee not in self.employees:
                raise ReferenceError("Owner does not exist")

            owner = self.employees[employee]
            owner.add_detail(
                AddressType(**kwargs)
            )

        if organisation_unit:

            if organisation_unit not in self.organisation_units:
                raise ReferenceError("Owner does not exist")

            owner = self.organisation_units[organisation_unit]
            owner.add_detail(
                AddressType(**kwargs)
            )


    def add_engagement(self, employee, organisation_unit, **kwargs):

        if employee not in self.employees:
            raise ReferenceError("Employee does not exist")

        if not organisation_unit in self.organisation_units:
            raise ReferenceError("Organisation unit does not exist")

        engagement = EngagementType(org_unit_ref=organisation_unit, **kwargs)

        owner = self.employees[employee]
        owner.add_detail(engagement)


    def add_association(self, employee, organisation_unit, **kwargs):

        if employee not in self.employees:
            raise ReferenceError("Employee does not exist")

        if not organisation_unit in self.organisation_units:
            raise ReferenceError("Organisation unit does not exist")

        association = AssociationType(org_unit=organisation_unit, **kwargs)

        owner = self.employees.get(employee)

        owner.add_detail(association)


    def add_role(self, employee, organisation_unit, **kwargs):

        if employee not in self.employees:
            raise ReferenceError("Employee does not exist")

        if not organisation_unit in self.organisation_units:
            raise ReferenceError("Organisation unit does not exist")

        role = RoleType(org_unit=organisation_unit, **kwargs)

        owner = self.employees.get(employee)

        owner.add_detail(role)

    def add_manager(self, employee, organisation_unit, **kwargs):

        if employee not in self.employees:
            raise ReferenceError("Employee does not exist")

        if not organisation_unit in self.organisation_units:
            raise ReferenceError("Organisation unit does not exist")

        manager = ManagerType(org_unit=organisation_unit, **kwargs)

        owner = self.employees.get(employee)

        owner.add_detail(manager)


    def add_leave(self, employee, **kwargs):

        if employee not in self.employees:
            raise ReferenceError("Employee does not exist")

        leave = LeaveType(**kwargs)

        owner = self.employees.get(employee)

        owner.add_detail(leave)


    def new_itsystem(self, identifier, **kwargs):

        if identifier in self.itsystems:
            raise ReferenceError("It system already exists")

        self.itsystems[identifier] = Itsystem(**kwargs)

    def join_itsystem(self, employee, **kwargs):
        if employee not in self.employees:
            raise ReferenceError("Employee does not exist")

        itsystem = ItsystemType(**kwargs)

        owner = self.employees.get(employee)

        owner.add_detail(itsystem)


    def create_default_facet_types(self, facet_defaults=facet_defaults):

        for user_key in facet_defaults:

            self.add_facet(
                identifier=user_key,
                user_key=user_key
            )


    def create_default_klasse_types(self, klasse_defaults=klasse_defaults):

        for identifier, facet_type_ref, kwargs in klasse_defaults:

            self.add_klasse(
                identifier=identifier,
                facet_type_ref=facet_type_ref,
                **kwargs
            )


    def import_all(self, DataStore=DataStore):

        # Before
        defaults = True


        # Insert
        store = DataStore()

        for identifier, organisation in self.organisation.items():
            storing = store.import_organisation(organisation)

            print(storing)



