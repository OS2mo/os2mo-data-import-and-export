# -- coding: utf-8 --

from os2mo_data_import.mora_data_types import *
from os2mo_data_import.mox_data_types import *
from os2mo_data_import.defaults import facet_defaults, klasse_defaults

import json
from urllib.parse import urljoin
from requests import Session
from uuid import uuid4

# Default settings
MOX_BASE = "http://localhost:8080"
MORA_BASE = "http://localhost:5000"


class HttpUtility(Session):

    def __init__(self):
        super().__init__()

class TestHttpUtility(object):
    def __init__(self):
        pass


class ImportUtility(object):

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
        self.inserted_facet_map = {}
        self.inserted_klasse_map = {}
        self.inserted_itsystem_map = {}
        self.inserted_org_unit_map = {}
        self.inserted_employee_map = {}

    def insert_organisation(self, identifier, organisation):

        self.insert_mox_data("organisation", identifier, organisation)

    def import_organisation(self, reference, organisation):
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

        resource = "organisation/organisation"

        integration_data = self._integration_data(
            resource=resource,
            reference=reference,
            payload={}
        )

        # Set integration data
        if integration_data:
            organisation.integration_data = integration_data

        payload = organisation.build()

        organisation_uuid = integration_data.get('uuid', None)

        self.organisation_uuid = self.insert_mox_data(
            resource=resource,
            data=payload,
            uuid=organisation_uuid
        )

        # Compatibility
        self.global_validity = {
            "from": organisation.date_from,
            "to": organisation.date_to
        }

        self.date_from = organisation.date_from
        self.date_to = organisation.date_to

        return self.organisation_uuid

    def import_klassifikation(self, reference, klassifikation):
        if not isinstance(klassifikation, Klassifikation):
            raise TypeError("Not of type Klassifikation")

        resource = "klassifikation/klassifikation"

        integration_data = self._integration_data(
            resource=resource,
            reference=reference,
            payload={}
        )

        # Set integration data
        if integration_data:
            klassifikation.integration_data = integration_data

        klassifikation.organisation_uuid = self.organisation_uuid

        payload = klassifikation.build()

        klassifikation_uuid = integration_data.get('uuid', None)

        self.klassifikation_uuid = self.insert_mox_data(
            resource=resource,
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

        if not isinstance(facet, Facet):
            raise TypeError("Not of type Facet")

        resource = "klassifikation/facet"

        integration_data = self._integration_data(
            resource=resource,
            reference=reference,
            payload={}
        )

        # Set integration data
        if integration_data:
            facet.integration_data = integration_data

        facet.organisation_uuid = self.organisation_uuid
        facet.klassifikation_uuid = self.klassifikation_uuid

        # NEED TO BE FIXED
        facet.date_from = self.date_from
        facet.date_to = self.date_to

        payload = facet.build()

        print(payload)

        facet_uuid = integration_data.get('uuid', None)

        self.inserted_facet_map[reference] = self.insert_mox_data(
            resource=resource,
            data=payload,
            uuid=facet_uuid
        )

        return self.inserted_facet_map[reference]

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

        if not isinstance(klasse, Klasse):
            raise TypeError("Not of type Facet")

        uuid = klasse.uuid
        facet_ref = klasse.facet_type_ref

        print("FACET REF")
        print(facet_ref)


        facet_uuid = self.inserted_facet_map.get(facet_ref)

        if not facet_uuid:
            print(klasse)
            error_message = "Facet ref: {ref} does not exist".format(
                ref=facet_ref
            )
            raise KeyError(error_message)

        resource = "klassifikation/klasse"

        integration_data = self._integration_data(
            resource=resource,
            reference=reference,
            payload={}
        )

        if integration_data:
            klasse.integration_data = integration_data

        klasse.organisation_uuid = self.organisation_uuid
        klasse.facet_uuid = facet_uuid
        klasse.date_from = self.date_from
        klasse.date_to = self.date_to

        payload = klasse.build()

        if 'uuid' in integration_data:
            klasse_uuid = integration_data['uuid']
            assert(uuid is None or klasse_uuid == uuid)
        else:
            if uuid is None:
                klasse_uuid = None
            else:
                klasse_uuid = uuid[0] # Internal representation is a 1-element tuple

        import_uuid = self.insert_mox_data(
            resource="klassifikation/klasse",
            data=payload,
            uuid=klasse_uuid
        )

        assert(uuid is None or import_uuid == klasse_uuid)
        self.inserted_klasse_map[reference] = import_uuid

        return self.inserted_klasse_map[reference]

    def import_itsystem(self, reference, itsystem):
        """
        Insert an itsystem object

        :param itsystem:
        Itsystem data object (dict)

        :returns:
        Inserted UUID (str)
        """

        if not isinstance(itsystem, Itsystem):
            raise TypeError("Not of type Itsystem")

        resource = 'organisation/itsystem'

        integration_data = self._integration_data(resource, reference, {})

        if integration_data:
            itsystem.integration_data = integration_data

        if 'uuid' in integration_data:
            itsystem_uuid = integration_data['uuid']
        else:
            itsystem_uuid = None

        itsystem.organisation_uuid = self.organisation_uuid
        itsystem.date_from = self.date_from
        itsystem.date_to = self.date_to

        payload = itsystem.build()

        self.inserted_itsystem_map[reference] = self.insert_mox_data(
            resource=resource,
            data=payload,
            uuid=itsystem_uuid
        )

        return self.inserted_itsystem_map[reference]

    def import_org_unit(self, reference, organisation_unit, details=[]):
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

        if not isinstance(organisation_unit, OrganisationUnitType):
            raise TypeError("Not of type OrganisationUnitType")

        if reference in self.inserted_org_unit_map:
            print("The organisation unit has already been inserted")
            return False

        resource = 'organisation/organisationenhed'

        integration_data = self._integration_data(
            resource=resource,
            reference=reference,
            payload={},
            encode_integration=False
        )

        # payload = self.build_mo_payload(organisation_unit_data)
        parent_ref = organisation_unit.parent_ref
        if parent_ref:
            parent_uuid = self.inserted_org_unit_map.get(parent_ref)
            organisation_unit.parent_uuid = parent_uuid

        if not organisation_unit.parent_uuid:
            organisation_unit.parent_uuid = self.organisation_uuid

        type_ref_uuid = self.inserted_klasse_map.get(
            organisation_unit.type_ref
        )

        organisation_unit.type_ref_uuid = type_ref_uuid

        # Build details (if any)
        organisation_unit.details = [
            self.build_detail(detail)
            for detail in details
        ]

        if organisation_unit.details:
            print("========= DETAILS ==========")
            print(organisation_unit.details)

        payload = organisation_unit.build()

        print("========= PAYLOAD ===========")
        print(payload)

        uuid = self.insert_mora_data(
            resource="service/ou/create",
            data=payload
        )

        if 'uuid' in integration_data:
            assert (uuid == integration_data['uuid'])
        if not uuid:
            raise ConnectionError("Something went wrong")

        # Add to the inserted map
        self.inserted_org_unit_map[reference] = uuid

        return uuid

    def import_employee(self, reference, employee, details=[]):
        if not isinstance(employee, EmployeeType):
            raise TypeError("Not of type EmployeeType")

        print("============ Employees =============")

        resource = 'organisation/organisationenhed'

        integration_data = self._integration_data(
            resource=resource,
            reference=reference,
            payload={},
            encode_integration=False
        )

        # Build details (if any)
        employee.details = [
            self.build_detail(detail)
            for detail in details
        ]

        employee.org_uuid = self.organisation_uuid
        payload = employee.build()

        print("=========== PAYLOAD ==============")
        print(payload)

        ## MARKER ##
        resource = 'organisation/organisationenhed'

        integration_data = self._integration_data(
            resource=resource,
            reference=reference,
            payload={},
            encode_integration=False
        )

        # We unconditionally create or update the user, this should
        # ensure that we alwas updated with correct current information.
        mora_resource = "service/e/create"
        uuid = self.insert_mora_data(
            resource=mora_resource,
            data=integration_data
        )

        if 'uuid' in integration_data:
            assert (uuid == integration_data['uuid'])

        # Add uuid to the inserted employee map
        self.inserted_employee_map[reference] = uuid

        return uuid

    def build_detail(self, detail):

        detail.type_ref_uuid = self.inserted_klasse_map.get(
            detail.type_ref
        )

        if hasattr(detail, "job_function_ref"):
            detail.job_function_uuid = self.inserted_klasse_map.get(
                detail.job_function_ref
            )

        if hasattr(detail, "address_type_ref"):
            detail.address_type_uuid = self.inserted_klasse_map.get(
                detail.address_type_ref
            )

        if hasattr(detail, "manager_level_ref"):
            detail.manager_level_uuid = self.inserted_klasse_map.get(
                detail.manager_level_ref
            )

        if hasattr(detail, "org_unit_ref"):
            detail.org_unit_uuid = self.inserted_org_unit_map.get(
                detail.org_unit_ref
            )

        # PLACEHOLDER FOR ADDRESS METADATA
        if hasattr(detail, "address_type_meta"):
            detail.address_type_meta = {
                "uuid": str(uuid4())
            }

        return detail.build()




    def insert_mox_data(self, resource, data, uuid=None):

        print(data)
        print(resource)

        if not uuid:
            uuid = uuid4()
        return str(uuid)

    def insert_mora_data(self, resource, data, uuid=None):
        print(data)
        print(resource)

        if not uuid:
            uuid = uuid4()

        return str(uuid)



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
