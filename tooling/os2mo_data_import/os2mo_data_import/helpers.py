from os2mo_data_import.mora_data_types import *
from os2mo_data_import.mox_data_types import *
from os2mo_data_import.utilities import ImportUtility
from os2mo_data_import.defaults import facet_defaults


class ImportHelper(object):

    def __init__(self, system_name="Import", end_marker="_|-STOP",
                 mox_base="http://localhost:8080", mora_base="http://localhost:5000",
                 store_integration_data=False, create_defaults=True,
                 ImportUtility=ImportUtility):

        # Import Utility
        self.store = ImportUtility(
            mox_base=mox_base,
            mora_base=mora_base,
            system_name=system_name,
            end_marker=end_marker,
            store_integration_data=store_integration_data
        )

        self.organisation = None
        self.klassifikation = None

        self.klasse_objects = {}
        self.facet_objects = {}
        self.addresses = []
        self.itsystems = {}

        self.organisation_units = {}
        self.employees = {}

        # Compatibility map
        self.available_types = {
            "klasse": "klasse_objects",
            "facet": "facet_objects",
            "organisation_unit": "organisation_units",
            "employee": "employees"
        }

        self.organisation_unit_details = {}
        self.employee_details = {}

        # Create default facet and klasse
        if create_defaults:
            self.create_default_facet_types()

    def get(self, object_type, object_reference):
        available = self.export(object_type)
        return available.get(object_reference)

    def export(self, object_type):
        available = self.available_types.keys()

        if object_type not in available:
            raise TypeError(
                "Cannot check for this type, available types: {}".format(
                    available
                )
            )

        object_type = self.available_types.get(object_type)

        return getattr(self, object_type)

    def check_if_exists(self, object_type, object_reference):

        attribute = self.export(object_type)

        if not attribute.get(object_reference):
            return False

        return True

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

        if "name" not in kwargs:
            kwargs["name"] = identifier

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

    def import_organisation_units_recursively(self, reference, org_unit):

        # Insert parents first!
        parent_ref = org_unit.parent_ref

        if parent_ref and parent_ref not in self.store.inserted_org_unit_map:
            parent_unit = self.organisation_units.get(parent_ref)

            # Insert parent first
            self.import_organisation_units_recursively(parent_ref, parent_unit)

        # Now insert actual units
        details = self.organisation_unit_details.get(reference)

        self.store.import_org_unit(
            reference=reference,
            organisation_unit=org_unit,
            details=details
        )

    def import_all(self):

        # Insert Organisation
        print('Will now import organisation')
        self.store.import_organisation(*self.organisation)

        # Insert Klassifikation
        print('Will now import klassifikation')
        self.store.import_klassifikation(*self.klassifikation)

        # Insert Facet
        print('Will now import facet')
        for identifier, facet in self.facet_objects.items():
            self.store.import_facet(identifier, facet)

        # Insert Klasse
        print('Will now import klasse')
        for identifier, klasse in self.klasse_objects.items():
            self.store.import_klasse(identifier, klasse)

        # Insert Itsystem
        print('Will now import IT-systems')
        for identifier, itsystem in self.itsystems.items():
            self.store.import_itsystem(identifier, itsystem)

        # Insert Organisation Units
        print('Will now import org units')
        for identifier, org_unit in self.organisation_units.items():
            self.import_organisation_units_recursively(identifier, org_unit)


        # Insert Employees
        print('Will now import employees')
        for identifier, employee in self.employees.items():

            details = self.employee_details.get(identifier)
            self.store.import_employee(
                reference=identifier,
                employee=employee,
                details=details
            )
