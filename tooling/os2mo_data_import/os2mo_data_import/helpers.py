#
# Copyright (c) 2017-2019, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

from os2mo_data_import.mora_data_types import *
from os2mo_data_import.mox_data_types import *
from os2mo_data_import.utilities import ImportUtility
from os2mo_data_import.defaults import facet_defaults


class ImportHelper(object):
    """
    The entry point of an import is the ImportHelper.

    Example:
        os2mo = ImportHelper(
            create_defaults=True,
            store_integration_data=True
        )

        os2mo.import_all()

    :param system_name:
    The main identifier for the specific import and/or integration (str)
    It will be stored as the main identifier for integration data,
    allowing for consecutive imports without causing conflicts in the database.

    :param end_marker:
    A semi unique marker which marks the end of the system name value (str)
    Consider the following example,
    where the system name is "Import" and the value is "ACME TM".
    The endmarker defaults to "_|-STOP".

    The data (integration data) is stored as a json string:

        { "Import": "ACME TM_|-STOP" }

    This is an attempt to eliminate collisions with "common" values.

    :param mox_base:
    The base url of the mox backend (str)
    E.g. http://mox.magenta.dk

    :param mora_base:
    The base url of the mora backend (str)
    E.g. http://mora.magenta.dk

    :param store_integration_data:
    Actually store the integration data in the database (bool)
    Any integration should only import store_integration_data=True.

    :param create_defaults:
    Create the default set of "facet" types (bool)
    For more information on the actual types, please see the defaults module.

    :param ImportUtility:
    The actual ImportUtility class which is used to insert mapped objects (class)
    via the MOX/OIO Rest interface

    """

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
        """
        Get a mapped object by type and reference.

        Example:
            os2mo.get("employee", "Luke Skywalker")

        :param object_type:
        Reference to type of the map (str)
        This can be either of the following:
         - klasse
         - facet
         - organisation_unit
         - employee

        :param object_reference:
        Reference to the identifier of the object in the map (str)

        :return:
        The selected object (object)
        """
        available = self.export(object_type)
        return available.get(object_reference)

    def export(self, object_type):
        """
        Compatibitlity method to fetch all objects by type.

        :param object_type:
        Reference to the identifier of the object in the map (str)

        :return:
        A list containing all mapped objects with selected type (list<object>)
        """
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
        """
        Helper method to check for the existence of a mapped object
        by type and reference.

        :param object_type:
        Reference to type of the map (str)

        :param object_reference:
        Reference to the identifier of the object in the map (str)

        :return:
        """

        attribute = self.export(object_type)

        if not attribute.get(object_reference):
            return False

        return True

    def get_details(self, owner_type, owner_ref, type_id=None):
        """
        Retrieve a list of details of either an employee or organisation unit.

        :param owner_type:
        Select either employee or organisation unit (str)

        :param owner_ref:
        Reference to the identifier of the employee or organisation unit (str)

        :param type_id:
        Optionally it can be specified which type of details
        should be returned (rather than the full list). (str)

        Example:
            os2mo.get_details("employee", "Luke Skywalker", "engagement")

        :return:
        Returns a list of details objects (object)
        """

        if owner_type == "organisation_unit":
            details = self.organisation_unit_details[owner_ref]
        elif owner_type == "employee":
            details = self.employee_details[owner_ref]
        else:
            raise ReferenceError(
                "Owner type must be either employee or organisation_unit"
            )

        if not details:
            details = []

        if type_id:
            details = [
                detail
                for detail in details
                if detail.type_id == type_id
            ]

        return details

    def create_validity(self, date_from, date_to):
        """
        Shorthand to create validity from the date_from and date_to attributes.

        :param date_from:
        Stringified start date (str)

        :param date_to:
        Stringified start end (str)

        :return:
        Returns validity key/value pairs (dict)
        """

        if not date_from or not date_to:
            raise AssertionError("Date is not specified, cannot create validity")

        return {
            "from": date_from,
            "to": date_to
        }

    def add_organisation(self, identifier, **kwargs):
        """
        Add an Organisation object to the map
        See available parameters for mox_data_types:Organisation

        :param identifier:
        A user defined identifier (str)
        If name is not set, the identifier is used as the name.
        """

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
        """
        Add a Klasse object to the map
        See available parameters for mox_data_types:Klasse

        :param identifier:
        A user defined identifier (str)
        """

        if identifier in self.klasse_objects:
            raise ReferenceError("Unique constraint - Klasse identifier exists")

        if "user_key" not in kwargs:
            kwargs["user_key"] = identifier

        self.klasse_objects[identifier] = Klasse(**kwargs)

    def add_facet(self, identifier, **kwargs):
        """
        Add a Facet object to the map
        See available parameters for mox_data_types:Facet

        :param identifier:
        A user defined identifier (str)
        """

        if identifier in self.facet_objects:
            raise ReferenceError("Unique constraint - Facet identifier exists")

        self.facet_objects[identifier] = Facet(**kwargs)


    def add_organisation_unit(self, identifier, **kwargs):
        """
        Add a OrganisationUnit object to the map
        See available parameters for mora_data_types:OrganisationUnitType

        :param identifier:
        A user defined identifier (str)
        """

        if identifier in self.organisation_units:
            raise ReferenceError("Identifier exists")

        if "name" not in kwargs:
            kwargs["name"] = identifier

        self.organisation_units[identifier] = OrganisationUnitType(**kwargs)
        self.organisation_unit_details[identifier] = []

    def add_employee(self, identifier, **kwargs):
        """
        Add a Employee object to the map
        See available parameters for mora_data_types:EmployeeType

        :param identifier:
        A user defined identifier (str)
        """

        if identifier in self.employees:
            raise ReferenceError("Identifier exists")

        if "name" not in kwargs:
            kwargs["name"] = identifier

        self.employees[identifier] = EmployeeType(**kwargs)
        self.employee_details[identifier] = []

    def add_address_type(self, organisation_unit=None, employee=None, **kwargs):
        """
        Add a Address object to the map
        See available parameters for mora_data_types:AddressType

        :param organisation_unit:
        Reference to the user defined identifer of the parent unit (str)

        :param employee:
        Reference to the user defined identifer of the employee (str)
        """

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
        """
        Add a Engagement object to the map
        See available parameters for mora_data_types:EngagementType

        :param organisation_unit:
        Reference to the user defined identifer of the parent unit (str)

        :param employee:
        Reference to the user defined identifer of the employee (str)
        """

        if employee not in self.employees:
            raise ReferenceError("Employee does not exist")

        if not organisation_unit in self.organisation_units:
            raise ReferenceError("Organisation unit does not exist")

        engagement = EngagementType(org_unit_ref=organisation_unit, **kwargs)

        self.employee_details[employee].append(engagement)


    def add_association(self, employee, organisation_unit, **kwargs):
        """
        Add a Association object to the map
        See available parameters for mora_data_types:AssociationType

        :param organisation_unit:
        Reference to the user defined identifer of the parent unit (str)

        :param employee:
        Reference to the user defined identifer of the employee (str)
        """

        association = AssociationType(org_unit_ref=organisation_unit, **kwargs)

        self.employee_details[employee].append(association)


    def add_role(self, employee, organisation_unit, **kwargs):
        """
        Add a Role object to the map
        See available parameters for mora_data_types:RoleType

        :param organisation_unit:
        Reference to the user defined identifer of the parent unit (str)

        :param employee:
        Reference to the user defined identifer of the employee (str)
        """

        if employee not in self.employees:
            raise ReferenceError("Employee does not exist")

        if not organisation_unit in self.organisation_units:
            raise ReferenceError("Organisation unit does not exist")

        role = RoleType(org_unit=organisation_unit, **kwargs)

        self.employee_details[employee].append(role)

    def add_manager(self, employee, organisation_unit, **kwargs):
        """
        Add a Manager object to the map
        See available parameters for mora_data_types:ManagerType

        :param organisation_unit:
        Reference to the user defined identifer of the parent unit (str)

        :param employee:
        Reference to the user defined identifer of the employee (str)
        """

        if employee not in self.employees:
            raise ReferenceError("Employee does not exist")

        if not organisation_unit in self.organisation_units:
            raise ReferenceError("Organisation unit does not exist")

        manager = ManagerType(org_unit=organisation_unit, **kwargs)

        self.employee_details[employee].append(manager)


    def add_leave(self, employee, **kwargs):
        """
        Add a Leave object to the map
        See available parameters for mora_data_types:LeaveType

        :param employee:
        Reference to the user defined identifer of the employee (str)
        """

        if employee not in self.employees:
            raise ReferenceError("Employee does not exist")

        leave = LeaveType(**kwargs)

        self.employee_details[employee].append(leave)


    def new_itsystem(self, identifier, **kwargs):
        """
        Add a new Itsystem object to the map
        See available parameters for mox_data_types:Itsystem

        :param identifier:
        A user defined identifier (str)
        """

        if identifier in self.itsystems:
            raise ReferenceError("It system already exists")

        self.itsystems[identifier] = Itsystem(**kwargs)

    def join_itsystem(self, employee, **kwargs):
        """
        Join Itsystem means adding a ItsystemType detail to the map
        See available parameters for mora_data_types:ItsystemType

        :param employee:
        Reference to the user defined identifer of the employee (str)
        """
        if employee not in self.employees:
            raise ReferenceError("Employee does not exist")

        itsystem = ItsystemType(**kwargs)

        self.employee_details[employee].append(itsystem)


    def create_default_facet_types(self, facet_defaults=facet_defaults):
        """
        Add the default list of Facet type objects to the map
        For more information, please see the "facet_defaults" list
        of the defaults module.

        :param facet_defaults:
        Default list of Facet types (list)
        This may be overridden at run time
        """

        for user_key in facet_defaults:

            self.add_facet(
                identifier=user_key,
                user_key=user_key
            )

    def import_organisation_units_recursively(self, reference, org_unit):
        """
        Begin importing all the organisation unit objects
        from the map recursively.
        Organisation units without a parent reference
        automatically become "root" units.

        :param reference:
        Reference to the user defined identifer of the unit (str)

        :param org_unit:
        The OrganisationUnitType object (object)
        """

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
        """
        The import method begins importing all objects
        obtained in the maps in the following order:

        Organisation object
        Klassifikation object (auto)
        Facet objects
        Klasse objects

        OrganisationUnit objects
        Employees objects
        """

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
