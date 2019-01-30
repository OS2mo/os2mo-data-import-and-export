# -- coding: utf-8 --

from os2mo_data_import.mora_data_types import *
from os2mo_data_import.mox_data_types import *


class ImportUtility(object):

    def __init__(self):

        self.organisation = None
        self.klassifikation = None

        self.klasse_objects = {}
        self.facet_objects = {}
        self.addresses = []
        self.itsystems = {}

        self.organisation_units = {}
        self.employees = {}

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

        self.organisation = Organisation(name=name, **kwargs)
        self.klassifikation = Klassifikation(user_key=name, parent_name=name, description="umbrella")

    def add_klasse(self, identifier, **kwargs):

        if identifier in self.klasse_objects:
            raise ReferenceError("Unique constraint - Klasse identifier exists")

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
