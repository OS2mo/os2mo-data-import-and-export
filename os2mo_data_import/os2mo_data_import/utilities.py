#
# Copyright (c) Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

import logging
from uuid import uuid4, UUID
from urllib.parse import urljoin
from requests import Session, HTTPError
from datetime import datetime, timedelta

from integration_abstraction.integration_abstraction import IntegrationAbstraction

from os2mo_data_import.mora_data_types import (
    OrganisationUnitType,
    TerminationType,
    EngagementTerminationType,
    EmployeeType
)

from os2mo_data_import.mox_data_types import (
    Organisation,
    Klassifikation,
    Facet,
    Klasse,
    Itsystem
)
from os2mo_helpers.mora_helpers import MoraHelper

logger = logging.getLogger("moImporterUtilities")


class ImportUtility(object):
    """
    ImportUtility
    TODO: This class relies heavily on asserts, which are gennerally not wanted in
    this context. We should create meaningfull exceptions and raise these when
    needed.
    """

    def __init__(self, system_name, end_marker, mox_base, mora_base,
                 demand_consistent_uuids, store_integration_data=False,
                 dry_run=False):

        # Import Params
        self.demand_consistent_uuids = demand_consistent_uuids
        self.store_integration_data = store_integration_data
        if store_integration_data:
            self.ia = IntegrationAbstraction(mox_base, system_name, end_marker)

        # Service endpoint base
        self.mox_base = mox_base
        self.mora_base = mora_base

        # Session
        self.mh = MoraHelper(self.mora_base, use_cache=False)
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

        :param str reference: Reference to the user defined identifier
        :param object organisation: Organisation object

        :returns: Inserted UUID
        :rtype: str/uuid
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

        if organisation.uuid is not None:
            organisation_uuid = organisation.uuid
        else:
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
        """
        Begin import of klassifikation

        :param str reference: Reference to the user defined identifier
        :param object klassifikation: Klassifikation object

        :returns: Inserted UUID
        :rtype: str/uuid
        """
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
        Begin import of facet

        :param str reference: Reference to the user defined identifier
        :param object facet: Facet object

        :returns: Inserted UUID
        :rtype: str/uuid
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

        Begin import of klassifikation

        :param str reference: Reference to the user defined identifier
        :param object organisation: Organisation object

        :returns: Inserted UUID
        :rtype: str/uuid
        """

        if not isinstance(klasse, Klasse):
            raise TypeError("Not of type Facet")

        uuid = klasse.uuid
        facet_ref = klasse.facet_type_ref

        facet_uuid = self.inserted_facet_map.get(facet_ref)

        if not facet_uuid:
            error_message = "Facet ref: {} does not exist for {}".format(
                facet_ref, klasse
            )
            logger.error(error_message)
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
        assert(uuid is None or import_uuid == str(klasse_uuid))
        self.inserted_klasse_map[reference] = import_uuid

        return self.inserted_klasse_map[reference]

    def import_itsystem(self, reference, itsystem):
        """
        Create IT System

        :param str reference: Reference to the user defined identifier
        :param object organisation: Organisation object

        :returns: Inserted UUID
        :rtype: str/uuid
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
        Insert organisation unit and details

        .. note::
            Optional data objects are relational objects which
            belong to the organisation unit, such as an address type

        :param str reference: Reference to the user defined identifier
        :param object organisation_unit: Organisation object
        :param list details: List of details
        :returns: Inserted UUID
        :rtype: str/uuid
        """

        if not isinstance(organisation_unit, OrganisationUnitType):
            raise TypeError("Not of type OrganisationUnitType")

        if reference in self.inserted_org_unit_map:
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

        if hasattr(organisation_unit, "time_planning_ref"):
            organisation_unit.time_planning_ref_uuid = self.inserted_klasse_map.get(
                organisation_unit.time_planning_ref
            )

        if hasattr(organisation_unit, 'org_unit_level_ref'):
            organisation_unit.org_unit_level_uuid = self.inserted_klasse_map.get(
                organisation_unit.org_unit_level_ref
            )

        organisation_unit.type_ref_uuid = type_ref_uuid

        payload = organisation_unit.build()
        payload = self._integration_data(
            resource=resource,
            reference=reference,
            payload=payload,
            encode_integration=False
        )

        if 'uuid' in payload:
            if payload['uuid'] in self.existing_uuids:
                logger.info('Re-import org-unit: {}'.format(payload['uuid']))
                re_import = 'NO'
                resource = 'details/edit'
                payload_keys = list(payload.keys())
                payload['data'] = {}
                for key in payload_keys:
                    payload['data'][key] = payload[key]
                    del payload[key]
                payload['type'] = 'org_unit'
            else:
                re_import = 'NEW'
                logger.info('New unit - Forced uuid: {}'.format(payload['uuid']))
                resource = 'ou/create'
        else:
            re_import = 'NEW'
            logger.info('New unit, random uuid')
            resource = 'ou/create'

        logger.debug('Unit payload: {}'.format(payload))

        uuid = self.insert_mora_data(
            resource=resource,
            data=payload
        )

        if 'uuid' in payload:
            assert (uuid == payload['uuid'])
        if not uuid:
            raise ConnectionError("Something went wrong")

        # Add to the inserted map
        self.inserted_org_unit_map[reference] = uuid

        data = {}
        data['address'] = self.mh._get_detail(uuid, 'address', object_type='ou')

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

            found_hit = self._payload_compare(build_detail, data)
            logger.debug('Found hit for {}: {}'.format(uuid, found_hit))
            if not found_hit and re_import == 'NO':
                re_import = 'YES'

            # TODO: SHOLD WE UPDATE FROM TO TODAY IN CASE OF RE-IMPORT?
            if re_import == 'YES':
                valid_from = datetime.now().strftime('%Y-%m-%d')  # today
                build_detail['validity']['from'] = valid_from
            details_payload.append(build_detail)

        if re_import == 'YES':
            logger.info('Terminating details for unit {}'.format(uuid))
            for item in data['address']:
                self._terminate_details(item['uuid'], 'address')
        if re_import in ('YES', 'NEW'):
            logger.info('Re-import unit: {}'.format(re_import))
            self.insert_mora_data(
                resource="details/create",
                data=details_payload
            )

        return uuid

    def import_employee(self, reference, employee, details=[]):
        """
        Import employee

        :param str reference: Reference to the user defined identifier
        :param object employee: Employee object
        :param list details: List of details
        :returns: Inserted UUID
        :rtype: str/uuid
        """
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

        if 'uuid' in payload and payload['uuid'] in self.existing_uuids:
            logger.info('Re-import employee {}'.format(payload['uuid']))
            re_import = 'NO'
        else:
            re_import = 'NEW'
            logger.info('New employee, uuid {}'.format(payload.get('uuid')))

        # We unconditionally create or update the user, this should
        # ensure that we are always updated with correct current information.
        mora_resource = "e/create"
        uuid = self.insert_mora_data(
            resource=mora_resource,
            data=integration_data
        )

        if 'uuid' in integration_data:
            assert (uuid == integration_data['uuid'])

        # Add uuid to the inserted employee map
        self.inserted_employee_map[reference] = uuid

        data = {}
        data['it'] = self.mh._get_detail(uuid, 'it')
        data['role'] = self.mh._get_detail(uuid, 'role')
        data['leave'] = self.mh._get_detail(uuid, 'leave')
        data['address'] = self.mh._get_detail(uuid, 'address')
        data['manager'] = self.mh._get_detail(uuid, 'manager')
        data['engagement'] = self.mh._get_detail(uuid, 'engagement')
        data['association'] = self.mh._get_detail(uuid, 'association')

        # In case of en explicit termination, we terminate the employee or
        # employment and return imidiately.
        for detail in details:
            if isinstance(detail, TerminationType):
                logger.info(
                    'Explicit termination of eng {} from {}'.format(
                        uuid, detail.date_from
                    )
                )
                self._terminate_employee(uuid, date_from=detail.date_from)
                return uuid
            if isinstance(detail, EngagementTerminationType):
                logger.info(
                    'Explicit termination of eng {}'.format(
                        detail.engagement_uuid
                    )
                )
                self._terminate_details(detail.engagement_uuid, 'engagement')
                return uuid

        if details:
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

                # If we do not have existing data, the new data should be imported
                if len(data[detail_payload['type']]) == 0 and re_import == 'NO':
                    re_import = 'UPDATE'
                elif data[detail_payload['type']]:
                    found_hit = self._payload_compare(detail_payload, data)
                    if not found_hit:
                        re_import = 'YES'
                additional_payload.append(detail_payload)

            for item in additional_payload:
                valid_from = item['validity']['from']
                valid_to = item['validity']['to']
                now = datetime.now()
                py_from = datetime.strptime(valid_from, '%Y-%m-%d')
                if valid_to is not None:
                    py_to = datetime.strptime(valid_to, '%Y-%m-%d')
                else:
                    py_to = datetime.strptime('2200-01-01', '%Y-%m-%d')

                logger.debug(
                    'Py-from:{}, Py-to:{}, Now:{}'.format(py_from, py_to, now)
                )
                if re_import == 'YES' and py_from < now and py_to > now:
                    logger.debug('Updating valid_from')
                    valid_from = datetime.now().strftime('%Y-%m-%d')  # today
                    item['validity']['from'] = valid_from

            logger.info('Re-import: {}'.format(re_import))

            if re_import == 'YES':
                logger.info('Non-explicit termination: {}'.format(uuid))
                self._terminate_employee(uuid)

            if re_import in ('YES', 'NEW', 'UPDATE'):
                self.insert_mora_data(
                    resource="details/create",
                    data=additional_payload
                )

        return uuid

    def build_detail(self, detail, employee_uuid=None):
        """
        Build detail payload

        :param MoType detail: Detail object

        .. note::
            A detail can be one of the following types:

                - address
                - asso
                - role
                - itsystem
                - engagement
                - manager

            :Reference: :mod:`os2mo_data_import.mora_data_types`

        :param str employee_uuid: (Option) Employee uuid if it exists

        :return: Detail POST data payload
        :rtype: dict
        """

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

            if not uuid:
                if self.store_integration_data:
                    klasse_res = 'klassifikation/klasse'
                    uuid = self.ia.find_object(klasse_res, getattr(detail, check_value))
                else:
                    # print('Detail: {}, check_value: {}'.format(detail, check_value))
                    pass

            setattr(detail, set_value, uuid)

        # Uncommon attributes
        if hasattr(detail, "visibility_ref"):
            detail.visibility_ref_uuid = self.inserted_klasse_map.get(
                detail.visibility_ref
            )

        if hasattr(detail, 'primary_ref'):
            detail.primary_uuid = self.inserted_klasse_map.get(
                detail.primary_ref
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
                if 'uuid' in payload:
                    if self.demand_consistent_uuids:
                        assert(payload['uuid'] == uuid)
                    else:
                        payload['uuid'] = uuid
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
                logger.error(
                    'Mox put. Response: {}, data'.format(response.text, data)
                )
                raise HTTPError("Inserting mox data failed")

        else:
            response = self.session.post(
                url=service_url,
                json=data
            )

            if response.status_code != 201:
                logger.error(
                    'Mox post. Response: {}, data'.format(response.text, data)
                )

                raise HTTPError("Inserting mox data failed")

        response_data = response.json()
        return response_data["uuid"]

    def insert_mora_data(self, resource, data, uuid=None):

        # TESTING
        if self.dry_run:
            uuid = uuid4()
            return str(uuid)

        response = self.mh._mo_post(
            url=resource,
            payload=data,
            force=True,
        )

        if response.status_code == 400:
            error = response.json()['description']
            if error.find('does not give raise to a new registration') > 0:
                uuid_start = error.find('with id [')
                uuid = error[uuid_start+9:uuid_start+45]
                try:
                    UUID(uuid, version=4)
                except ValueError:
                    raise Exception('Unable to read uuid')
            else:
                logger.error(
                    'MO post. Error: {}, data: {}'.format(error, data)
                )
                raise HTTPError("Inserting mora data failed")
        elif response.status_code not in (200, 201):
            logger.error(
                'MO post. Response: {}, data: {}'.format(response.text, data)
            )
            raise HTTPError("Inserting mora data failed")
        else:
            uuid = response.json()
        return uuid

    def _terminate_employee(self, uuid, date_from=None):
        endpoint = 'e/{}/terminate'
        yesterday = datetime.now() - timedelta(days=1)
        if date_from:
            to = date_from
        else:
            to = yesterday.strftime('%Y-%m-%d')

        payload = {
            'terminate_all': True,
            'validity': {
                'to': to
            }
        }
        resource = endpoint.format(uuid)

        self.insert_mora_data(
            resource=resource,
            data=payload
        )
        return uuid

    def _terminate_details(self, uuid, detail_type):
        logger.info('Terminate detail {}:  {}'.format(uuid, detail_type))
        yesterday = datetime.now() - timedelta(days=1)
        payload = {
            'type': detail_type,
            'uuid': uuid,
            'validity': {
                'to': yesterday.strftime('%Y-%m-%d')
            }
        }
        logger.debug('Terminate detail payload: {}'.format(payload))
        try:
            self.insert_mora_data(
                resource='details/terminate',
                data=payload
            )
        except HTTPError:
            logger.info('Tried to terminate a non-existing uuid')
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

        logger.debug(
            'Payload compare. item_payload: {}, data: {}'.format(item_payload, data)
        )

        found_hit = False
        if data_type == 'engagement':
            # In priciple, we should be able to re-calculate the hash stored in
            # integration data and compare directly from that.
            for data_item in data[data_type]:
                if (self._std_compare(item_payload, data_item, 'job_function') and
                    item_payload['user_key'] == data_item['user_key'] and
                    item_payload['fraction'] == data_item['fraction']):
                    found_hit = True

        elif data_type == 'role':
            for data_item in data[data_type]:
                if self._std_compare(item_payload, data_item, 'role_type'):
                    found_hit = True
                    logger.debug('Found hit in role')
        elif data_type == 'leave':
            for data_item in data[data_type]:
                if ((data_item['validity']['from'] ==
                     item_payload['validity']['from']) and

                    (data_item['validity']['to'] ==
                     item_payload['validity']['to'])):
                    found_hit = True
                    logger.debug('Found hit in leave')
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
                    logger.debug('Found hit in it')
        elif data_type == 'address':
            for data_item in data[data_type]:
                if (
                    (data_item['validity']['from'] ==
                     item_payload['validity']['from']) and

                    (data_item['validity']['to'] ==
                     item_payload['validity']['to']) and

                    (data_item['value'] == item_payload['value'])
                ):
                    found_hit = True
                    logger.debug('Found hit in adress')
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
                    logger.debug('Found  hit in manager')
        elif data_type == 'association':
            for data_item in data[data_type]:
                if self._std_compare(item_payload, data_item, 'association_type'):
                    found_hit = True
                    logger.debug('Found hit in association')
        else:
            raise Exception('Uknown detail!')
        logger.info('Found hit: {}'.format(found_hit))
        return found_hit
