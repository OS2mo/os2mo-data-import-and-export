#
# Copyright (c) 2017-2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#


class MoType():

    uuid = None
    type_id = None
    insert_data = True

    def __init__(self):

        self.payload = {}

        self.date_from = None
        self.date_to = None

    def _build_payload(self):

        if not isinstance(self.payload, dict):
            raise TypeError("Cannot build non-dict types")

        # Add type:
        if self.type_id:
            self.payload["type"] = self.type_id

        # Add validity:
        self.payload["validity"] = {
            "from": self.date_from,
            "to": self.date_to
        }

        return self.payload


class AddressType(MoType):

    type_id = "address"

    def __init__(self, type_ref, date_from, date_to=None, value=None, uuid=None):
        super().__init__()

        self.value = value
        self.uuid = uuid
        self.type_ref = type_ref
        self.type_ref_uuid = None

        # SPECIAL
        self.address_type = None

        self.date_from = date_from
        self.date_to = date_to

    def build(self):

        if not self.value and not self.uuid:
            raise ValueError("Either value or uuid must be passed")

        # Add address_type meta data from the "facet" endpoint
        if not self.address_type:
            raise ValueError("Cannot build payload, missing type ref")

        self.payload["address_type"] = self.address_type

        if self.value:
            self.payload["value"] = self.value
        else:
            self.payload["uuid"] = self.uuid

        return self._build_payload()


class EngagementType(MoType):

    type_id = "engagement"

    def __init__(self, type_ref, org_unit, job_function,
                 date_from, date_to=None, uuid=None):
        super().__init__()

        self.uuid = uuid

        self.org_unit = org_unit
        self.job_function = job_function

        self.type_ref = type_ref
        self.type_ref_uuid = None

        self.org_unit_uuid = None
        self.job_function_uuid = None

        self.date_from = date_from
        self.date_to = date_to

    def build(self):

        if self.uuid:
            self.payload["uuid"] = self.uuid

        # Reference the parent org unit uuid
        if not self.org_unit_uuid:
            raise ReferenceError("Reference to parent org unit is missing")

        self.payload["org_unit"] = {
              "uuid": self.org_unit_uuid
        }

        # Reference the job function uuid
        if not self.job_function_uuid:
            raise ReferenceError("Reference to job function is missing")

        self.payload["job_function"] = {
            "uuid": self.job_function_uuid
        }

        # Reference the engagement type uuid
        if not self.type_ref_uuid:
            raise ReferenceError("Reference to engagenment type is missing")

        self.payload["engagement_type"] = {
            "uuid": self.type_ref_uuid
        }

        return self._build_payload()


class AssociationType(MoType):

    type_id = "association"

    def __init__(self, type_ref, org_unit, job_function,
                 address_uuid, date_from, date_to=None):
        super().__init__()

        self.org_unit = org_unit
        self.org_unit_uuid = None

        self.job_function = job_function
        self.job_function_uuid = None

        self.type_ref = type_ref
        self.type_ref_uuid = None

        self.address_uuid = address_uuid
        self.address_type_meta = None

        self.date_from = date_from
        self.date_to = date_to

    def build(self):

        if not self.org_unit_uuid:
            raise ReferenceError("Reference to org_unit_uuid is missing")

        self.payload["org_unit"] = {
              "uuid": self.org_unit_uuid
        }

        if not self.job_function_uuid:
            raise ReferenceError("Reference to job_function_uuid is missing")

        self.payload["job_function_uuid"] = {
              "uuid": self.job_function_uuid
        }

        if not self.type_ref_uuid:
            raise ReferenceError("Reference to association_type_uuid is missing")

        self.payload["association_type_uuid"] = {
              "uuid": self.type_ref_uuid
        }

        if self.address_uuid:
            self.payload["address"] = {
                "uuid": self.address_uuid,
                "address_type": self.address_type_meta
            }

        return self._build_payload()


class ItsystemType(MoType):

    type_id = "it"

    def __init__(self, type_ref, date_from, date_to=None, user_key=None):
        super().__init__()

        self.user_key = user_key
        self.type_ref = type_ref
        self.type_ref_uuid = None

        self.date_from = date_from
        self.date_to = date_to

    def build(self):

        self.payload["user_key"] = {
            "uuid": self.user_key
        }

        self.payload["itsystem"] = {
            "uuid": self.type_ref_uuid
        }

        return self._build_payload()


class LeaveType(MoType):

    type_id = "leave"

    def __init__(self, type_ref, date_from, date_to=None):
        super().__init__()

        self.type_ref = type_ref
        self.type_ref_uuid = None

        self.date_from = date_from
        self.date_to = date_to

    def build(self):

        self.payload["leave_type"] = {
              "uuid": self.type_ref_uuid
            }

        return self._build_payload()


class RoleType(MoType):

    type_id = "role"

    def __init__(self, org_unit, type_ref, date_from, date_to=None):
        super().__init__()

        self.org_unit = org_unit
        self.org_unit_uuid = None

        self.type_ref = type_ref
        self.type_ref_uuid = None

        self.date_from = date_from
        self.date_to = date_to

    def build(self):

        self.payload["org_unit"] = {
            "uuid": self.org_unit_uuid
        }

        self.payload["role_type"] = {
            "uuid": self.type_ref_uuid
        }

        return self._build_payload()


class ManagerType(MoType):
    type_id = "manager"

    def __init__(self, org_unit, type_ref, manager_level,
                 address_uuid, responsibility, date_from, date_to=None, uuid=None):
        super().__init__()

        self.uuid = uuid

        self.org_unit = org_unit
        self.org_unit_uuid = None

        self.type_ref = type_ref
        self.type_ref_uuid = None

        self.manager_level = manager_level
        self.manager_level_uuid = None

        if not isinstance(responsibility, list):
            raise TypeError("Responsabilities must be passed as a list")

        self.responsibility = responsibility

        self.address_uuid = address_uuid

    def build(self):
        if self.uuid:
            self.payload["uuid"] = self.uuid

        self.payload["org_unit"] = {
            "uuid": self.org_unit_uuid
        }

        self.payload["manager_type"] = {
            "uuid": self.type_ref_uuid
        }

        self.payload["manager_level"] = {
            "uuid": self.manager_level_uuid
        }

        self.payload["responsibility"] = [
            {
                "uuid": responsibility_uuid
            }
            for responsibility_uuid in self.responsibility
        ]

        self.payload["address"] = self.address_uuid

        return self._build_payload()


class OrganisationUnitType(MoType):

    def __init__(self, name, parent_ref, type_ref,
                 date_from, date_to=None, uuid=None, user_key=None):
        super().__init__()

        self.name = name
        self.uuid = uuid
        self.user_key = user_key

        self.parent_ref = parent_ref
        self.parent_uuid = None

        self.type_ref = type_ref
        self.type_ref_uuid = None

        self.optional_data = []

        self.date_from = date_from
        self.date_to = date_to

    def build(self):

        if not self.parent_uuid:
            raise ReferenceError("UUID of the parent organisation is missing")

        if not self.type_ref_uuid:
            raise ReferenceError("UUID of the unit type is missing")

        self.payload = {
            "uuid": self.uuid,
            "user_key": self.user_key,
            "name": self.name,
            "parent": {
                "uuid": self.parent_uuid
            },
            "org_unit_type": {
                "uuid": self.type_ref_uuid
            }
        }

        return self._build_payload()


class EmployeeType(MoType):

    def __init__(self, name, cpr_no, org=None, uuid=None, user_key=None):
        super().__init__()

        self.name = name
        self.cpr_no = cpr_no

        self.uuid = uuid
        self.user_key = user_key

        self.optional_data = []

        self.org = org
        self.org_uuid = None

    def build(self):

        if not self.org_uuid:
            raise ReferenceError("UUID of the organisation is missing")

        self.payload = {
            "uuid": self.uuid,
            "user_key": self.user_key,
            "name": self.name,
            "cpr_no": self.cpr_no,
            "org": {
                "uuid": self.org_uuid
            }
        }