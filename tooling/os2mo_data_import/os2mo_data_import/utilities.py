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


class TestSession():
    pass


class HttpUtility():

    def __init__(self, mox_base, mora_base, dry_run=False):

        self.mox_base = mox_base
        self.mora_base = mora_base

        if dry_run:
            self.session = Session()
        else:
            self.session = TestSession()

    # def insert_mox_data(self, data):
    #
    #     if not isinstance(data, dict):
    #         raise TypeError("Inccorect data type")
    #
    #     service_url = self.mox_base
    #
    #     return self.session.post(
    #         url=service_url,
    #         json=data
    #     )

    def insert_mox_data(self, resource, data):
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

            response_data = {
                "uuid": str(
                    uuid4()
                )
            }
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

            response_data = str(
                uuid4()
            )
        else:
            response = self.session.post(url=service, json=data)
            response_data = response.json()

        return response_data


class ImportUtility(object):

    def __init__(self, dry_run=False):

        self.dry_run = dry_run

        # Inserted uuid maps
        self.inserted_facet_map = {}
        self.inserted_klasse_map = {}
        self.inserted_org_unit_map = {}
        self.inserted_employee_map = {}
        self.inserted_itsystem_map = {}

    def import_organisation(self, exported_data):

        print(exported_data)

        return "234"

    def import_klassifikation(self, parent_name):

        print(parent_name)

        return "234"

    def import_facet(self, owner_ref, facet_data):

        print(owner_ref, facet_data)

        return "234234234234"

    def import_klasse(self, owner_ref, klasse_data):
        uuid = "234234234234"
        print(owner_ref, klasse_data)
        self.inserted_klasse_map[owner_ref] = uuid
        return uuid

    def import_itsystem(self, owner_ref, itsystem):
        print(owner_ref, itsystem)

        return "234234234234"

    def run_opt(self, data):

        ref = data.type_ref
        data.type_ref_uuid = self.get_klasse(ref)

        if data.type_id == "address":
            data.address_type = {
                "uuid": data.type_ref_uuid,
                "scope": "DAR"
            }

        print("TYPE REF: {}".format(data.type_ref))

        return data.build()

    def import_organisation_unit(self, owner_ref, organisation_unit):

        organisation_unit.unit_type_uuid = self.get_klasse(organisation_unit.type_ref)
        organisation_unit.parent_uuid = "sdfsdfsdfsdf"

        payload = self.run_opt(organisation_unit)

        payload["details"] = [
            self.run_opt(data)
            for data in organisation_unit.optional_data
        ]

        print(payload)

        return organisation_unit.unit_type_uuid

    def import_employee(self, owner_ref, employee):
        print(owner_ref, employee.__dict__)

        return "234234234234"

    def get_klasse(self, reference):
        klasse = self.inserted_klasse_map.get(reference)

        if not klasse:
            raise ReferenceError("The klasse does not exist")

        return klasse


    def import_all(self, org):

        if not isinstance(org, Organisation):
            raise AssertionError("Object is not an instance of Organisation")

        # Set global validity
        self.global_validity = org.validity

        # HOTFIX: temporary fix for nested organisation units
        self.org = org

        # Insert Organisation
        org_export = self.org.export()
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
        for identifier, itsystem in org.Itsystem.export():
            uuid = self.import_itsystem(identifier, itsystem)
            print("Inserted itsystem: %s" % uuid)

        # Insert OrganisationUnit
        for identifier, organisation_unit in org.OrganisationUnit.export():
            uuid = self.import_organisation_unit(identifier, organisation_unit)
            print("Inserted organisation_unit: %s" % uuid)

        # Insert Employee
        for identifier, employee in org.Employee.export():
            uuid = self.import_employee(identifier, employee)
            print("Inserted employee: %s" % uuid)