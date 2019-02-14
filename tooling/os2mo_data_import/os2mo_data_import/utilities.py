# -- coding: utf-8 --
import json
import copy
from uuid import uuid4
from urllib.parse import urljoin
from requests import Session, HTTPError
from datetime import datetime, timedelta

from integration_abstraction.integration_abstraction import IntegrationAbstraction
from os2mo_data_import.mora_data_types import OrganisationUnitType
from os2mo_data_import.mora_data_types import EmployeeType

from os2mo_data_import.mox_data_types import Organisation
from os2mo_data_import.mox_data_types import Klassifikation
from os2mo_data_import.mox_data_types import Itsystem
from os2mo_data_import.mox_data_types import Facet
from os2mo_data_import.mox_data_types import Klasse


class ImportUtility(object):

    def __init__(self, system_name, end_marker, mox_base, mora_base,
                 store_integration_data=False, dry_run=False):

        # Import Params
        self.store_integration_data = store_integration_data
        if store_integration_data:
            self.ia = IntegrationAbstraction(mox_base, system_name, end_marker)

        # Service endpoint base
        self.mox_base = mox_base
        self.mora_base = mora_base

        # Session
        self.session = Session()

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

        # Deprecated
        self.dry_run = dry_run

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

        organisation.integration_data = integration_data

        payload = organisation.build()

        organisation_uuid = integration_data.get('uuid', None)

        self.organisation_uuid = self.insert_mox_data(
            resource=resource,
            data=payload,
            uuid=organisation_uuid
        )

        # Global validity
        self.date_from = organisation.date_from
        self.date_to = organisation.date_to

        return self.organisation_uuid

    def import_klassifikation(self, reference, klassifikation):
        if not isinstance(klassifikation, Klassifikation):
            raise TypeError("Not of type Klassifikation")

        resource = "klassifikation/klassifikation"

        klassifikation.organisation_uuid = self.organisation_uuid

        integration_data = self._integration_data(
            resource=resource,
            reference=reference,
            payload={}
        )

        klassifikation.integration_data = integration_data
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

        facet.organisation_uuid = self.organisation_uuid
        facet.klassifikation_uuid = self.klassifikation_uuid

        # NEED TO BE FIXED
        facet.date_from = self.date_from
        facet.date_to = self.date_to

        integration_data = self._integration_data(
            resource=resource,
            reference=reference,
            payload={}
        )

        facet.integration_data = integration_data
        payload = facet.build()

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

        facet_uuid = self.inserted_facet_map.get(facet_ref)

        if not facet_uuid:
            print(klasse)
            error_message = "Facet ref: {ref} does not exist".format(
                ref=facet_ref
            )
            raise KeyError(error_message)

        resource = "klassifikation/klasse"

        klasse.organisation_uuid = self.organisation_uuid
        klasse.facet_uuid = facet_uuid
        klasse.date_from = self.date_from
        klasse.date_to = self.date_to

        integration_data = self._integration_data(
            resource=resource,
            reference=reference,
            payload={}
        )

        if 'uuid' in integration_data:
            klasse_uuid = integration_data['uuid']
            assert(uuid is None or klasse_uuid == uuid)
        else:
            if uuid is None:
                klasse_uuid = None
            else:
                klasse_uuid = uuid

        klasse.integration_data = integration_data
        payload = klasse.build()

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

        itsystem.organisation_uuid = self.organisation_uuid
        itsystem.date_from = self.date_from
        itsystem.date_to = self.date_to

        integration_data = self._integration_data(
            resource=resource,
            reference=reference,
            payload={}
        )

        if 'uuid' in integration_data:
            itsystem_uuid = integration_data['uuid']
        else:
            itsystem_uuid = None

        itsystem.integration_data = integration_data
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

        payload = organisation_unit.build()

        integration_data = self._integration_data(
            resource=resource,
            reference=reference,
            payload=payload,
            encode_integration=False
        )

        uuid = self.insert_mora_data(
            resource="service/ou/create",
            data=integration_data
        )

        if 'uuid' in integration_data:
            assert (uuid == integration_data['uuid'])
        if not uuid:
            raise ConnectionError("Something went wrong")

        # Add to the inserted map
        self.inserted_org_unit_map[reference] = uuid

        # Details
        # Build details (if any)
        details_payload = []

        for detail in details:

            detail.org_unit_uuid = uuid

            date_from = detail.date_from

            if not date_from:
                date_from = organisation_unit.date_from

            build_detail = self.build_detail(
                detail=detail
            )

            if not build_detail:
                continue

            details_payload.append(build_detail)

        self.insert_mora_data(
            resource="service/details/create",
            data=details_payload
        )

        return uuid

    def import_employee(self, reference, employee, details=[]):
        if not isinstance(employee, EmployeeType):
            raise TypeError("Not of type EmployeeType")

        employee.org_uuid = self.organisation_uuid
        payload = employee.build()
        mox_resource = 'organisation/bruger'

        integration_data = self._integration_data(
            resource=mox_resource,
            reference=reference,
            payload=payload,
            encode_integration=False
        )

        if 'uuid' in integration_data:
            print('Re-import employee')
        else:
            print("NEW EMPLOYEEE")

        # We unconditionally create or update the user, this should
        # ensure that we alwas updated with correct current information.
        mora_resource = "service/e/create"
        uuid = self.insert_mora_data(
            resource=mora_resource,
            data=integration_data
        )
        print(uuid)
        print()
        if 'uuid' in integration_data:
            assert (uuid == integration_data['uuid'])

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

        if details:
            complete_additional_payload = []
            additional_payload = []
            for detail in details:
                if not detail.date_from:
                    detail.date_from = self.date_from

                # Create payload (as dict)
                detail_payload = self.build_detail(
                    detail=detail,
                    employee_uuid=uuid
                )

                if not detail_payload:
                    continue

                if detail.type_id in data.keys():
                    found_hit = self._payload_compare(detail_payload, data)
                else:
                    found_hit = False

                new_item_payload = copy.deepcopy(detail_payload)

                valid_from = new_item_payload['validity']['from']
                if datetime.strptime(valid_from, '%Y-%m-%d') < datetime.now():
                    valid_from = datetime.now().strftime('%Y-%m-%d')  # today

                valid_to = new_item_payload['validity']['to']

                if valid_to:
                    future = datetime.strptime(valid_to, '%Y-%m-%d') > datetime.now()
                else:
                    future = False

                if not valid_to or future:
                    new_item_payload['validity']['from'] = valid_from
                    # new_item_payload['validity']['from'] = today
                    complete_additional_payload.append(new_item_payload)
                    # Clean this up. We do not need a long and a short list
                    # of payloads, we need to know if something changes and thus
                    # if we need to terminate and re-hire the employee
                    # if not found_hit. This awaits fixing the current issues in MO.
                additional_payload.append(detail_payload)

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

    def build_detail(self, detail, employee_uuid=None):

        if employee_uuid:
            detail.person_uuid = employee_uuid

        common_attributes = [
            ("type_ref", "type_ref_uuid"),
            ("job_function_ref", "job_function_uuid"),
            ("address_type_ref", "address_type_uuid"),
            ("manager_level_ref", "manager_level_uuid")
        ]

        for check_value, set_value in common_attributes:
            if not hasattr(detail, check_value):
                continue

            uuid = self.inserted_klasse_map.get(
                getattr(detail, check_value)
            )

            setattr(detail, set_value, uuid)

        # Uncommon attributes
        if hasattr(detail, "visibility_ref"):
            detail.visibility_ref_uuid = self.inserted_klasse_map.get(
                detail.visibility_ref
            )

        if hasattr(detail, "org_unit_ref"):
            detail.org_unit_uuid = self.inserted_org_unit_map.get(
                detail.org_unit_ref
            )

        if hasattr(detail, "organisation_uuid"):
            detail.organisation_uuid = self.organisation_uuid

        if hasattr(detail, "itsystem_ref"):
            detail.itsystem_uuid = self.inserted_itsystem_map.get(
                detail.itsystem_ref
            )

        if hasattr(detail, "responsibilities"):
            detail.responsibilities = [
                self.inserted_klasse_map[reference]
                for reference in detail.responsibility_list
            ]

        return detail.build()

    def _integration_data(self, resource, reference, payload={},
                          encode_integration=True):
        """
        Update the payload with integration data. Checks if an object with this
        integration data already exists. In this case the uuid of the exisiting
        object is put into the payload. If a supplied uuid is inconsistent with the
        uuid found from integration data, an exception is raised.

        :param resource:
        LoRa resource URL.

        :param referece:
        Unique label that will be stored in the integration data to identify the
        object on re-import.

        :param payload:
        The supplied payload will be updated with values for integration and uuid (if
        the integration data was found from an earlier import). For MO objects,
        payload will typically be pre-populated and will then be ready for import
        when returned. For MOX objects, the initial payload  will typically be empty,
        and the returned values can be fed to the relevant adapter.

        :param encode_integration:
        If True, the integration data will be returned in json-encoded form.

        :return:
        The original payload updated with integration data and object uuid, if the
        object was already imported.
        """
        # TODO: We need to have a list of all objects with integration data to
        # be able to make a list of objects that has disappeared
        if self.store_integration_data:
            uuid = self.ia.find_object(resource, reference)
            if uuid:
                payload['uuid'] = uuid
                self.existing_uuids.append(uuid)

            payload['integration_data'] = self.ia.integration_data_payload(
                resource,
                reference,
                uuid,
                encode_integration
            )
        return payload

    def insert_mox_data(self, resource, data, uuid=None):

        service_url = urljoin(
            base=self.mox_base,
            url=resource
        )

        if uuid:
            update_url = "{service}/{uuid}".format(
                service=service_url,
                uuid=uuid
            )

            response = self.session.put(
                url=update_url,
                json=data
            )

            if response.status_code != 200:
                # DEBUG
                # TODO: Implement logging
                print("============ ERROR ===========")
                print(resource)
                print(
                    json.dumps(data, indent=2)
                )

                raise HTTPError("Inserting mox data failed")

        else:
            response = self.session.post(
                url=service_url,
                json=data
            )

            if response.status_code != 201:
                # DEBUG
                # TODO: Implement logging
                print("============ ERROR ===========")
                print(resource)
                print(
                    json.dumps(data, indent=2)
                )

                raise HTTPError("Inserting mox data failed")

        response_data = response.json()
        return response_data["uuid"]

    def insert_mora_data(self, resource, data, uuid=None):

        # TESTING
        if self.dry_run:
            uuid = uuid4()
            return str(uuid)

        params = {
            "force": 1
        }

        service_url = urljoin(
            base=self.mora_base,
            url=resource
        )

        response = self.session.post(
            url=service_url,
            json=data,
            params=params
        )

        response_data = response.json()

        if response.status_code not in (200, 201):

            # DEBUG
            # TODO: Implement logging
            print("============ ERROR ===========")
            print(resource)
            print(
                json.dumps(data, indent=2)
            )

            raise HTTPError("Inserting mora data failed")

        response_data = response.json()

        # Returns a string rather than a json object
        # Example: "0fd6a479-8569-42dd-9614-4aacb611306e"
        return response_data

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
        endpoint = 'service/e/{}/terminate'
        yesterday = datetime.now() - timedelta(days=1)
        payload = {
            'terminate_all': True,
            'validity': {
                'to': yesterday.strftime('%Y-%m-%d')
            }
        }
        resource = endpoint.format(uuid)

        self.insert_mora_data(
            resource=resource,
            data=payload
        )
        return uuid

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
