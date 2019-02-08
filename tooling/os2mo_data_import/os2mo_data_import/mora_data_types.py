#
# Copyright (c) 2017-2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#


class MoType():

    type_id = None
    insert_data = True

    def __init__(self):

        self.payload = {}

        self.date_from = None
        self.date_to = None

        # Compatibility:
        # Write details after unit or employee is stored
        self.person_uuid = None
        self.org_unit_uuid = None

    def _build_payload(self):

        if not isinstance(self.payload, dict):
            raise TypeError("Cannot build non-dict types")

        # Add type:
        if self.type_id:
            self.payload["type"] = self.type_id

        if self.person_uuid:
            self.payload["person"] = {
                "uuid": self.person_uuid
            }

        if self.org_unit_uuid:
            self.payload["org_unit"] = {
                "uuid": self.org_unit_uuid
            }

        # Add validity:
        self.payload["validity"] = {
            "from": self.date_from,
            "to": self.date_to
        }

        return self.payload

    def __repr__(self):
        return str(self.__dict__)


class AddressType(MoType):

    type_id = "address"

    def __init__(self, value, type_ref, date_from, date_to=None):
        super().__init__()

        self.value = value
        self.type_ref = type_ref
        self.type_ref_uuid = None

        # https://os2mo.readthedocs.io/en/development/api/address.html#writing
        self.organisation_uuid = None

        # SPECIAL
        self.address_type_meta = None

        self.date_from = date_from
        self.date_to = date_to

    def build(self):

        if not self.organisation_uuid:
            raise ValueError("Organisation UUID is missing")

        self.payload = {
            "value": self.value,
            "org": {
                "uuid": self.organisation_uuid
            },
            "address_type": {
                "uuid": self.type_ref_uuid
            }
        }

        return self._build_payload()


class EngagementType(MoType):

    type_id = "engagement"

    def __init__(self, org_unit_ref, job_function_ref, engagement_type_ref,
                 date_from, date_to=None):
        super().__init__()

        self.org_unit_ref = org_unit_ref
        self.org_unit_uuid = None

        self.type_ref = engagement_type_ref
        self.type_ref_uuid = None

        self.job_function_ref = job_function_ref
        self.job_function_uuid = None

        self.date_from = date_from
        self.date_to = date_to

    def build(self):

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

    def __init__(self, association_type_ref, org_unit_ref, job_function_ref,
                 date_from, date_to=None, address_uuid=None, address_type_ref=None):
        super().__init__()

        self.org_unit_ref = org_unit_ref
        self.org_unit_uuid = None

        self.job_function_ref = job_function_ref
        self.job_function_uuid = None

        self.type_ref = association_type_ref
        self.type_ref_uuid = None

        self.address_uuid = address_uuid

        # Workaround for address_type_ref
        self.address_type_ref = (address_type_ref or "AdressePostEmployee")
        self.address_type_uuid = None

        self.date_from = date_from
        self.date_to = date_to

    def build(self):

        if not self.org_unit_uuid:
            raise ReferenceError("Reference to org_unit_uuid is missing")

        if not self.job_function_uuid:
            raise ReferenceError("Reference to job_function_uuid is missing")

        if not self.type_ref_uuid:
            raise ReferenceError("Reference to association_type_uuid is missing")

        self.payload = {
            "org_unit": {
                "uuid": self.org_unit_uuid
            },
            "job_function": {
                "uuid": self.job_function_uuid
            },
            "association_type": {
                "uuid": self.type_ref_uuid
            }
        }

        if self.address_uuid:
            self.payload["address"] = {
                "uuid": self.address_uuid,
                "address_type": {
                    "uuid": self.address_type_uuid
                }
            }

        return self._build_payload()


class ItsystemType(MoType):

    type_id = "it"

    def __init__(self, user_key, itsystem_ref, date_from, date_to=None):
        super().__init__()

        self.user_key = user_key
        self.itsystem_ref = itsystem_ref
        self.itsystem_uuid = None

        self.date_from = date_from
        self.date_to = date_to

    def build(self):

        if not self.itsystem_uuid:
            raise ReferenceError("UUID of itsystem type is missing")

        self.payload = {
            "user_key": self.user_key,
            "itsystem": {
                "uuid": self.itsystem_uuid
            }
        }

        return self._build_payload()


class LeaveType(MoType):

    type_id = "leave"

    def __init__(self, leave_type_ref, date_from, date_to=None):
        super().__init__()

        self.type_ref = leave_type_ref
        self.type_ref_uuid = None

        self.date_from = date_from
        self.date_to = date_to

    def build(self):

        self.payload = {
            "leave_type": {
                "uuid": self.type_ref_uuid
            }
        }

        return self._build_payload()


class RoleType(MoType):

    type_id = "role"

    def __init__(self, org_unit, role_type_ref, date_from, date_to=None):
        super().__init__()

        self.org_unit_ref = org_unit
        self.org_unit_uuid = None

        self.type_ref = role_type_ref
        self.type_ref_uuid = None

        self.date_from = date_from
        self.date_to = date_to

    def build(self):

        self.payload = {
            "org_unit": {
                "uuid": self.org_unit_uuid
            },
            "role_type": {
                "uuid": self.type_ref_uuid
            }
        }

        return self._build_payload()


class ManagerType(MoType):
    type_id = "manager"

    def __init__(self, org_unit, manager_type_ref, manager_level_ref,
                 responsibility_list, date_from, date_to=None,
                 address_uuid=None, address_type_ref=None):
        super().__init__()

        self.org_unit_ref = org_unit
        self.org_unit_uuid = None

        self.type_ref = manager_type_ref
        self.type_ref_uuid = None

        self.manager_level_ref = manager_level_ref
        self.manager_level_uuid = None

        if not isinstance(responsibility_list, list):
            raise TypeError("Responsabilities must be passed as a list")

        self.responsibility_list = responsibility_list
        self.responsibilities = []

        self.address_uuid = address_uuid
        self.address_type_ref = address_type_ref
        self.address_type_uuid = None

        self.date_from = date_from
        self.date_to = date_to

    def build(self):

        self.payload = {
            "org_unit": {
                "uuid": self.org_unit_uuid
            },
            "manager_type": {
                "uuid": self.type_ref_uuid
            },
            "manager_level": {
                "uuid": self.manager_level_uuid
            },
            "responsibility": [
                {
                    "uuid": responsibility_uuid
                }
                for responsibility_uuid in self.responsibilities
            ]
        }

        if self.address_uuid:
            self.payload["address"] = {
                "uuid": self.address_uuid,
                "address_type": {
                    "uuid": self.address_type_uuid
                }
            }

        return self._build_payload()


class OrganisationUnitType(MoType):

    def __init__(self, name, type_ref, date_from, date_to=None,
                 user_key=None, parent_ref=None, uuid=None):
        super().__init__()

        self.uuid = uuid
        self.name = name
        self.user_key = (user_key or name)

        self.parent_ref = parent_ref
        self.parent_uuid = None

        self.type_ref = type_ref
        self.type_ref_uuid = None

        self.details = []

        self.date_from = date_from
        self.date_to = date_to

    def build(self):

        if not self.parent_uuid:
            raise ReferenceError("UUID of the parent organisation is missing")

        if not self.type_ref_uuid:
            raise ReferenceError("UUID of the unit type is missing")

        if self.uuid:
            self.payload["uuid"] = {
                "uuid": self.uuid
            }

        self.payload = {
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

        self.org = org
        self.org_uuid = None

    def build(self):

        if not self.org_uuid:
            raise ReferenceError("UUID of the organisation is missing")

        if self.uuid:
            self.payload["uuid"] = {
                "uuid": self.uuid
            }

        self.payload = {
            "name": self.name,
            "cpr_no": self.cpr_no,
            "org": {
                "uuid": self.org_uuid
            }
        }

        return self.payload