from os2mo_data_import.mora_data_types import *
from os2mo_data_import.mox_data_types import *
from os2mo_data_import.utilities import ImportUtility
from os2mo_data_import.defaults import facet_defaults, klasse_defaults


class ImportHelper(object):

    def __init__(self, create_defaults=True):

        self.organisation = ()
        self.klassifikation = ()

        self.klasse_objects = {}
        self.facet_objects = {}
        self.addresses = []
        self.itsystems = {}

        self.organisation_units = {}
        self.employees = {}

        self.organisation_unit_details = {}
        self.employee_details = {}

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

        self.organisation = (
            identifier,
            Organisation(name=name, **kwargs),
        )

        self.klassifikation = (
            identifier,
            Klassifikation(user_key=name, parent_name=name, description="umbrella")
        )

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
        self.organisation_unit_details[identifier] = []

    def add_employee(self, identifier, **kwargs):

        if identifier in self.employees:
            raise ReferenceError("Identifier exists")

        if "name" not in kwargs:
            kwargs["name"] = identifier

        self.employees[identifier] = EmployeeType(**kwargs)
        self.employee_details[identifier] = []

    def add_address_type(self, organisation_unit=None, employee=None, **kwargs):

        if not (organisation_unit or employee):
            raise ReferenceError("Either organisation unit or employee must be owner")

        if organisation_unit and employee:
            raise ReferenceError("Must reference either organisation unit or employee and not both")

        if employee:

            if employee not in self.employees:
                raise ReferenceError("Owner does not exist")

            self.employee_details[employee].append(
                AddressType(**kwargs)
            )

        if organisation_unit:

            if organisation_unit not in self.organisation_units:
                raise ReferenceError("Owner does not exist")

            self.organisation_unit_details[organisation_unit].append(
                AddressType(**kwargs)
            )

    def add_engagement(self, employee, organisation_unit, **kwargs):

        if employee not in self.employees:
            raise ReferenceError("Employee does not exist")

        if not organisation_unit in self.organisation_units:
            raise ReferenceError("Organisation unit does not exist")

        engagement = EngagementType(org_unit_ref=organisation_unit, **kwargs)

        self.employee_details[employee].append(engagement)


    def add_association(self, employee, organisation_unit, **kwargs):

        if employee not in self.employees:
            raise ReferenceError("Employee does not exist")

        if not organisation_unit in self.organisation_units:
            raise ReferenceError("Organisation unit does not exist")

        association = AssociationType(org_unit_ref=organisation_unit, **kwargs)

        self.employee_details[employee].append(association)


    def add_role(self, employee, organisation_unit, **kwargs):

        if employee not in self.employees:
            raise ReferenceError("Employee does not exist")

        if not organisation_unit in self.organisation_units:
            raise ReferenceError("Organisation unit does not exist")

        role = RoleType(org_unit=organisation_unit, **kwargs)

        self.employee_details[employee].append(role)

    def add_manager(self, employee, organisation_unit, **kwargs):

        if employee not in self.employees:
            raise ReferenceError("Employee does not exist")

        if not organisation_unit in self.organisation_units:
            raise ReferenceError("Organisation unit does not exist")

        manager = ManagerType(org_unit=organisation_unit, **kwargs)

        self.employee_details[employee].append(manager)


    def add_leave(self, employee, **kwargs):

        if employee not in self.employees:
            raise ReferenceError("Employee does not exist")

        leave = LeaveType(**kwargs)

        self.employee_details[employee].append(leave)


    def new_itsystem(self, identifier, **kwargs):

        if identifier in self.itsystems:
            raise ReferenceError("It system already exists")

        self.itsystems[identifier] = Itsystem(**kwargs)

    def join_itsystem(self, employee, **kwargs):
        if employee not in self.employees:
            raise ReferenceError("Employee does not exist")

        itsystem = ItsystemType(**kwargs)

        self.employee_details[employee].append(itsystem)


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


    def import_all(self, Utility=ImportUtility):

        # Before
        defaults = True


        # Insert
        store = Utility()

        # Insert Organisation
        identifier, organisation = self.organisation
        store.import_organisation(identifier, organisation)

        # Insert Klassifikation
        identifier, klassifikation = self.klassifikation
        store.import_klassifikation(identifier, klassifikation)

        # Insert Facet
        for identifier, facet in self.facet_objects.items():
            store.import_facet(identifier, facet)

        # Insert Klasse
        for identifier, klasse in self.klasse_objects.items():
            store.import_klasse(identifier, klasse)

        # Insert Itsystem
        print('Will now import IT-systems')
        for identifier, itsystem in self.itsystems.items():
            store.import_itsystem(identifier, itsystem)

        # Insert Organisation Units
        print('Will now import org units')
        for identifier, org_unit in self.organisation_units.items():


            parent_ref = org_unit.parent_ref

            if parent_ref and parent_ref not in store.inserted_org_unit_map:
                parent_unit = self.organisation_units.get(parent_ref)
                parent_details = self.organisation_unit_details.get(parent_ref)

                store.import_org_unit(
                    reference=parent_ref,
                    organisation_unit=parent_unit,
                    details=parent_details
                )

            details = self.organisation_unit_details.get(identifier)

            store.import_org_unit(
                reference=identifier,
                organisation_unit=org_unit,
                details=details
            )

        #
        # # Insert Employees
        # print('Will now import employees')
        # for identifier, employee in org.Employee.export():
        #     uuid = self.import_employee(
        #         reference=identifier,
        #         employee_data=employee["data"],
        #         optional_data=employee["optional_data"]
        #         )