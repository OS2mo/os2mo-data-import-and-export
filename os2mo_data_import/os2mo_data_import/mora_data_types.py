#
# Copyright (c) Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
import hashlib
import logging

logger = logging.getLogger("moImporterMoraTypes")


# TODO: This should be in some sort of global config
def mora_type_config(mox_base):
    MoType.mox_base = mox_base


class MoType():
    """
    Base class for all Mo (MORA) type objects

    :param str/uuid uuid: The object uuid

    .. note::
        This can be set to import the object with its current uuid.
        On a lower level this means that the current registration will be set to end
        now and a new registration of the same object is created with from date
        set to now.

    :param str type_id: Reference to the detail type

    .. note::
        A detail can be one of the following types:

            - address
            - asso
            - role
            - itsystem
            - engagement
            - manager

        :Reference: :mod:`os2mo_data_import.mora_data_types`

    :param str date_from: Start date e.g. "1982-01-01"
    :param str date_to: End date e.g. "1982-01-01"
    """

    def __init__(self):
        self.type_id = None

        self.payload = {}

        self.date_from = None
        self.date_to = None

        # Streamline
        # Force uuid on objects
        self.uuid = None

        # Compatibility:
        # Write details after unit or employee is stored
        self.person_uuid = None
        self.org_unit_uuid = None

        self.insert_data = True

    def _build_payload(self):
        """
        Create a POST data payload for os2mo (mora)

        As a minimum the date_from and date_to attributes must be set.
        Additionally must each set the type_id attribute.
        Optionally a person_uuid and org_unit_uuid can be set
        on "detail" class objects which belong to either
        an organisation unit or a employee.

        :return: POST data payload
        :rtype: dict
        """

        if not isinstance(self.payload, dict):
            raise TypeError("Cannot build non-dict types")

        if self.uuid:
            self.payload["uuid"] = self.uuid

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
        logger.debug('Mo payload: {}'.format(self.payload))
        return self.payload

    def __repr__(self):
        return str(self.__dict__)


class AddressType(MoType):
    """
    Address type detail which belongs to eit

    :param str value: The address value

    .. note::
        The value can be a phone number:
        value: 11223344

        or a reference to a DAR object:
        value: 62B99146-BA66-4563-B15E-A4F4B9000B58

    :param str type_ref: Reference to the Klasse object

    .. note::
        The os2mo representation of the values depends on its scope.
        The type_ref refers to a scope by referencing the Klasse object
        that contains the scope.

        :Reference: :class:`os2mo_data_import.mox_data_types.Klasse`

    :param bool visibility: Should the value be displayed
    :param str/uuid uuid: The object uuid
    :param str date_from: Start date e.g. "1982-01-01"
    :param str date_to: End date e.g. "1982-01-01"
    """

    def __init__(self, value, type_ref, date_from,
                 date_to=None, uuid=None, visibility=None):
        super().__init__()

        self.type_id = "address"
        self.uuid = uuid

        self.value = value
        self.type_ref = type_ref
        self.type_ref_uuid = None

        self.visibility_ref = visibility
        self.visibility_ref_uuid = None

        self.organisation_uuid = None

        # SPECIAL
        self.address_type_meta = None

        self.date_from = date_from
        self.date_to = date_to

    def build(self):
        """
        Apply class specific attributes before calling
        the underlying _build method to create the POST data payload.

        :return: POST data payload
        :rtype: dict
        """

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

        if self.visibility_ref:
            self.payload['visibility'] = {'uuid': self.visibility_ref_uuid}
        return self._build_payload()


class EngagementType(MoType):
    """
    Engagement type detail.
    This detail describes the current employment.

    :param str org_unit_ref: Reference to the parent organisation unit
    :param str engagement_type_ref: Reference to the type of employment

    Examples: "Full time", "Part time", "External", "Internal" etc.

    :param str job_function_ref: Reference to the "job title"

    Examples: "Bridge officer", "Science assistant", "Space dock cleaner" etc.

    :param str/uuid uuid: The object uuid
    :param str date_from: Start date e.g. "1982-01-01"
    :param str date_to: End date e.g. "1982-01-01"
    """

    def __init__(self, org_unit_ref, job_function_ref, engagement_type_ref,
                 date_from, date_to=None, uuid=None, user_key=None, fraction=None,
                 primary_ref=None, **kwargs):
        super().__init__()

        self.type_id = "engagement"
        self.uuid = uuid
        if user_key:
            self.user_key = user_key
        else:
            self.user_key = '-'

        if fraction:
            self.fraction = fraction
        else:
            self.fraction = None

        self.primary_ref = primary_ref
        self.primary_uuid = None

        self.org_unit_ref = org_unit_ref
        self.org_unit_uuid = None

        self.type_ref = engagement_type_ref
        self.type_ref_uuid = None

        self.job_function_ref = job_function_ref
        self.job_function_uuid = None

        self.date_from = date_from
        self.date_to = date_to

        self.extentions = kwargs

    def build(self):
        """
        Apply class specific attributes before calling
        the underlying _build method to create the POST data payload.

        :return: POST data payload
        :rtype: dict
        """

        # Reference the parent org unit uuid
        if not self.org_unit_uuid:
            raise ValueError("empty org_unit_uuid")

        self.payload["org_unit"] = {
              "uuid": self.org_unit_uuid
        }

        # We are not yet at a level, where we are ready to force uuid's on
        # engagements. Similarly, we do no attempts of sharing engagement objects
        # between application, currently the integration data support is limiited
        # to writng a unique key in the identifier for the engagement. For the
        # same reason, we do not actually bother to obey the store_integrationn_data
        # setting, the value is just always written.
        # The unique key is calculated as hash over the individual parts of the
        # engagement
        resource = 'organisation/organisationfunktion'
        hash_value = hashlib.sha256()
        hash_value.update(str(self.org_unit_ref).encode('utf-8'))
        hash_value.update(str(self.type_ref).encode('utf-8'))
        hash_value.update(str(self.job_function_ref).encode('utf-8'))
        if self.date_from:
            hash_value.update(self.date_from.encode('ascii'))
        if self.date_to:
            hash_value.update(self.date_to.encode('ascii'))

        if self.primary_uuid:
            self.payload['primary'] = {
                'uuid': self.primary_uuid
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
        if self.user_key:
            self.payload['user_key'] = self.user_key
        self.payload['fraction'] = self.fraction

        # Todo: Consider a check for the value of the keys.
        self.payload.update(self.extentions)

        return self._build_payload()


class AssociationType(MoType):
    """
    Engagement type detail.
    This detail describes the current employment.

    :param str org_unit_ref: Reference to the parent organisation unit.
    :param str engagement_type_ref: Reference to the type of employment.

    Examples: "Full time", "Part time", "External", "Internal" etc.

    :param str job_function_ref: Reference to the "job title".

    Examples: "Bridge officer", "Science assistant", "Space dock cleaner" etc.

    :param str association_type_ref: Reference to the type of association.

    .. note::
        There are several ways to define an association,
        for example it could be that an employee is associated with
        any given projects/tasks within the organisation unit.

         - Employee: Luke Skywalker
         - Organisation unit: Republic
         - Engagement: Jedi/Rebel scum
         - Association: Affairs on the moon of Endor

    :param str/uuid address_uuid: Reference to a DAR address object.

    :param str address_type_ref: Reference to the address type.

    .. important::
        This can only be a reference to a Klasse object with the scope: DAR.

    :param str user_key: User key for the association.
    :param str/uuid uuid: The object uuid.
    :param str date_from: Start date e.g. "1982-01-01".
    :param str date_to: End date e.g. "1982-01-01".
    """

    def __init__(self, association_type_ref, org_unit_ref, date_from,
                 date_to=None, address_uuid=None, address_type_ref=None,
                 uuid=None, user_key=None):
        super().__init__()

        self.type_id = "association"

        self.uuid = uuid
        if user_key:
            self.user_key = user_key
        else:
            self.user_key = '-'

        self.org_unit_ref = org_unit_ref
        self.org_unit_uuid = None

        self.type_ref = association_type_ref
        self.type_ref_uuid = None

        self.address_uuid = address_uuid

        # Workaround for address_type_ref
        self.address_type_ref = (address_type_ref or "AdressePostEmployee")
        self.address_type_uuid = None

        self.date_from = date_from
        self.date_to = date_to

    def build(self):
        """
        Apply class specific attributes before calling
        the underlying _build method to create the POST data payload.

        :return: POST data payload
        :rtype: dict
        """

        if not self.org_unit_uuid:
            raise ValueError("empty org_unit_uuid")

        self.payload = {
            "user_key": self.user_key,
            "org_unit": {
                "uuid": self.org_unit_uuid
            },
            "association_type": {
                "uuid": self.type_ref_uuid
            }
        }

        # TODO: We removed addresses from associations, right?
        if self.address_uuid:
            self.payload["address"] = {
                "uuid": self.address_uuid,
                "address_type": {
                    "uuid": self.address_type_uuid
                },
                'user_key': self.user_key
            }
        return self._build_payload()


class ItsystemType(MoType):
    """
    Creates a detail object that describes the connection
    with an existing it system.

    :param str itsystem_ref: Reference to the it system type
    :param str/uuid uuid: The object uuid
    :param str date_from: Start date e.g. "1982-01-01"
    :param str date_to: End date e.g. "1982-01-01"
    """

    def __init__(self, user_key, itsystem_ref, date_from, date_to=None, uuid=None):
        super().__init__()

        self.type_id = "it"

        self.uuid = uuid

        self.user_key = user_key
        self.itsystem_ref = itsystem_ref
        self.itsystem_uuid = None

        self.date_from = date_from
        self.date_to = date_to

    def build(self):
        """
        Apply class specific attributes before calling
        the underlying _build method to create the POST data payload.

        :return: POST data payload
        :rtype: dict
        """

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
    """
    Leave type detail describes a period of leave of absence
    of an associated employee.

    :param str leave_type_ref: Reference to the type of leave

    Examples: "Illness", "R & R", "Captivity", "Off-record secret mission"

    :param str/uuid uuid: The object uuid
    :param str date_from: Start date e.g. "1982-01-01"
    :param str date_to: End date e.g. "1982-01-01"
    """

    def __init__(self, leave_type_ref, engagement_uuid, date_from, date_to=None, uuid=None):
        super().__init__()

        self.type_id = "leave"

        self.uuid = uuid
        self.engagement_uuid = engagement_uuid

        self.type_ref = leave_type_ref
        self.type_ref_uuid = None

        self.date_from = date_from
        self.date_to = date_to

    def build(self):
        """
        Apply class specific attributes before calling
        the underlying _build method to create the POST data payload.

        :return: POST data payload
        :rtype: dict
        """

        self.payload = {
            "leave_type": {"uuid": self.type_ref_uuid},
            "engagement": {"uuid": self.engagement_uuid}
        }

        return self._build_payload()


class TerminationType(MoType):
    def __init__(self, date_from):
        self.date_from = date_from


class EngagementTerminationType(MoType):
    def __init__(self, engagement_uuid):
        self.engagement_uuid = engagement_uuid


class RoleType(MoType):
    """
    Role type detail describes a which role an employee holds
    with the associated organisation unit.

    :param str org_unit: Reference to the associated organisation unit
    :param str role_type_ref: Reference to the role type

    Examples: "Quartermaster", "Holder of keys", "Victory party organiser",

    :param str/uuid uuid: The object uuid
    :param str date_from: Start date e.g. "1982-01-01"
    :param str date_to: End date e.g. "1982-01-01"
    """

    def __init__(self, org_unit, role_type_ref, date_from, date_to=None, uuid=None):
        super().__init__()

        self.type_id = "role"

        self.uuid = uuid

        self.org_unit_ref = org_unit
        self.org_unit_uuid = None

        self.type_ref = role_type_ref
        self.type_ref_uuid = None

        self.date_from = date_from
        self.date_to = date_to

    def build(self):
        """
        Apply class specific attributes before calling
        the underlying _build method to create the POST data payload.

        :return: POST data payload
        :rtype: dict
        """

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
    """
    Role type detail describes a which role an employee holds
    with the associated organisation unit.

    :param str org_unit: Reference to the associated organisation unit

    :param str manager_type_ref: Reference to type of manager position

    Examples: "Jedi Master", "Emperor" etc.

    :param str manager_level_ref: Reference to the role type

    Examples: "Security level 5", "Bridge access" etc

    :param list responsibility_list: List of references to responsibilities

    Examples: "Bridge officer", "Squad leader", "Ground team coordination"

    :param str/uuid address_uuid: Reference to a DAR object
    :param str address_type_ref: Reference to the address type

    .. important::
        this can only be a reference to a Klasse object with the scope: DAR.

    :param str/uuid uuid: The object uuid
    :param str date_from: Start date e.g. "1982-01-01"
    :param str date_to: End date e.g. "1982-01-01"
    """

    def __init__(self, org_unit, manager_type_ref, manager_level_ref,
                 responsibility_list, date_from, date_to=None, user_key=None,
                 address_uuid=None, address_type_ref=None, uuid=None):
        super().__init__()

        self.type_id = "manager"

        self.uuid = uuid

        if user_key:
            self.user_key = user_key
        else:
            self.user_key = None

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
        """
        Apply class specific attributes before calling
        the underlying _build method to create the POST data payload.

        :return: POST data payload
        :rtype: dict
        """

        self.payload = {
            'user_key': self.user_key,
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
    """
    :param str name: Name of the organisation unit
    :param str type_ref: Reference to the organisation unit type/Klasse
    :param str user_key: (Optional) user key for internal reference
    :param str parent_ref: Reference to the parent organisation

    .. note::
        If this value is not set, the organisation becomes the parent.

    :param str/uuid uuid: The object uuid
    :param str date_from: Start date e.g. "1982-01-01"
    :param str date_to: End date e.g. "1982-01-01"
    """

    def __init__(self, name, type_ref, date_from, date_to=None,
                 user_key=None, time_planning_ref=None, org_unit_level_ref=None,
                 parent_ref=None, uuid=None):
        super().__init__()

        self.uuid = uuid
        self.name = name
        self.user_key = (user_key or name)

        self.parent_ref = parent_ref
        self.parent_uuid = None

        self.type_ref = type_ref
        self.type_ref_uuid = None

        self.org_unit_level_ref = org_unit_level_ref
        self.org_unit_level_uuid = None

        self.time_planning_ref = time_planning_ref
        self.time_planning_ref_uuid = None

        self.details = []

        self.date_from = date_from
        self.date_to = date_to

    def build(self):
        """
        Apply class specific attributes before calling
        the underlying _build method to create the POST data payload.

        :return: POST data payload
        :rtype: dict
        """

        if not self.parent_uuid:
            raise ReferenceError("UUID of the parent organisation is missing")

        if not self.type_ref_uuid:
            raise ValueError("empty type_ref_uuid")

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
        if self.time_planning_ref:
            self.payload["time_planning"] = {
                "uuid": self.time_planning_ref_uuid
            }

        if self.org_unit_level_ref:
            self.payload['org_unit_level'] = {
                'uuid': self.org_unit_level_uuid
            }

        return self._build_payload()


class EmployeeType(MoType):
    """
    A MO employee. Either name OR givenname and surname should be supplied.
    :param str name: Full name of the employee, if type is a tupe with two elements,
    they will be considered as givenname and surname.
    :param str givenname: Given name of the employee
    :param str surname: Surname of the employee
    :param str cpr_no: 10 digit CPR identifier code
    :param str user_key: (Optional) user key for internal reference
    :param str org: Reference to the organisation to which the employee belongs
    :param str/uuid uuid: The object uuid
    """

    def __init__(self, name, cpr_no, org=None, uuid=None, user_key=None,
                 seperate_names=False):
        super().__init__()

        if isinstance(name, tuple):
            self.givenname = name[0]
            self.surname = name[1]
        else:
            self.givenname = name.rsplit(" ", maxsplit=1)[0]
            self.surname = name[len(self.givenname):].strip()
        self.seperate_names = seperate_names

        self.cpr_no = cpr_no

        self.uuid = uuid
        if user_key:
            self.user_key = user_key
        else:
            self.user_key = str(name)

        self.org = org
        self.org_uuid = None

    def build(self):
        """
        Apply class specific attributes before calling
        the underlying _build method to create the POST data payload.

        :return: POST data payload
        :rtype: dict
        """

        if not self.org_uuid:
            raise ReferenceError("UUID of the organisation is missing")

        self.payload = {
            "user_key": self.user_key,
            "cpr_no": self.cpr_no,
            "org": {
                "uuid": self.org_uuid
            }
        }

        if self.seperate_names:
            self.payload['givenname'] = self.givenname
            self.payload['surname'] = self.surname
        else:
            self.payload['name'] = self.givenname + ' ' + self.surname

        return self._build_payload()
