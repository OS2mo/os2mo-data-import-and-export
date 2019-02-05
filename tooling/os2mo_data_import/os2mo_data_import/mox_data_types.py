# -- coding: utf-8 --


class Base(object):

    def __init__(self):

        self.date_from = "1900-01-01"
        self.date_to = "infinity"
        self.integration_data = {}

    def validate_integration_data(self):

        if not isinstance(self.integration_data, dict):
            raise TypeError("Integration data must be passed as dict")

        if "integration_data" not in self.integration_data:
            return str()

        return self.integration_data['integration_data']

    def create_validity(self):

        if not self.date_from or not self.date_to:
            raise AssertionError("Date is not specified, cannot create validity")

        return {
            "from": self.date_from,
            "to": self.date_to
        }

    def __repr__(self):
        return str(self.__dict__)


class Facet(Base):
    """
    Facet type - parent of klasse type
    In the current state of the os2mo application
    no other types than default values should be needed.

    """

    def __init__(self, user_key, uuid=None, organisation_uuid=None,
                 klassifikation_uuid=None, date_from=None, date_to=None):

        # Init parent
        super().__init__()

        self.uuid = uuid
        self.user_key = user_key

        self.organisation_uuid = str(organisation_uuid)
        self.klassifikation_uuid = str(klassifikation_uuid)

        self.date_from = (date_from or "1900-01-01")
        self.date_to = (date_to or "infinity")

    def build(self):

        properties = {
            "brugervendtnoegle": self.user_key,
            "integrationsdata": self.validate_integration_data(),
            "virkning": self.create_validity()
        }

        attributter = {
            "facetegenskaber": [properties]
        }

        relationer = {
            "ansvarlig": [
                {
                    "objekttype": "organisation",
                    "uuid": self.organisation_uuid,
                    "virkning": self.create_validity()
                }
            ],
            "facettilhoerer": [
                {
                    "objekttype": "klassifikation",
                    "uuid": self.klassifikation_uuid,
                    "virkning": self.create_validity()
                }
            ]
        }

        tilstande = {
            "facetpubliceret": [
                {
                    "publiceret": "Publiceret",
                    "virkning": self.create_validity()
                }
            ]
        }

        return {
            "attributter": attributter,
            "relationer": relationer,
            "tilstande": tilstande
        }


class Klasse(Base):
    """
    The Klasse class provides functionality for collecting
    user defined klasse meta objects for import.

    In the current state of the os2mo application
    the following two klasse objects must be created:

        {
            "user_key": "Telefon",
            "example": "20304060",
            "scope": "PHONE",
            "title": "Tlf"
        }

        {
            "user_key": "AdressePost",
            "example": "<UUID>",
            "scope": "DAR",
            "title": "Adresse"
        }

    The frontend application expects these two klasse objects to exist.
    These provide functionality for 2 required input fields.

    Note:
        The required objects are included in the list of defaults
        and are automatically created when running create_defaults().

    TODO:
        Add validation for the required types.
        Additionally check for all common types which are not created.

        The defaults should be additive and only created if the user
        has not created the required types.

    """

    def __init__(self, facet_type_ref, user_key, description=None,
                 example=None, scope=None, title=None, uuid=None,
                 date_from=None, date_to=None):
        """
        Add new facet to the storage map.
        In the context of the os2mo application,
        a new object does only require a unique user_key.

        :param identifier:
            User defined identifier

        :param facet_type_ref:
            User defined identifier

        :param user_key:
            (Required) user_key (str)
            Defaults to the value of the passed identifier.
            This can be set for internal reference.

        :param title:
            (Required) title (str)
            Defaults to the value of the passed identifier
            The value which will be displayed in the frontend application

        :param description:
            (Optional) description (str)
            This value is reserved for frontend tooltips.

        :param example:
            (Optional) example (str)
            This value is reserved for frontend placeholders.

        :param scope:
            (Optional) scope (str)
            This value is used for validation and field generation.
            E.g. scope: DAR will render a field with validation for address uuids

        :param uuid:
            (Optional) uuid (uuid)
            If uuid is provided, the object will be imported into LoRa with this
            uuid value.

        :return:
            Returns data as dict.

        """

        super().__init__()

        self.facet_type_ref = facet_type_ref
        self.user_key = user_key
        self.description = description
        self.scope = scope
        self.title = (title or user_key)
        self.example = example
        self.uuid = uuid

        self.organisation_uuid = None
        self.facet_uuid = None

        self.date_from = (date_from or "1900-01-01")
        self.date_to = (date_to or "infinity")

    def build(self):

        if not self.organisation_uuid:
            raise AssertionError(
                "Organisation uuid is missing - cannot build"
            )

        if not self.facet_uuid:
            raise AssertionError(
                "Facet uuid is missing - cannot build"
            )

        properties = {
            "brugervendtnoegle": self.user_key,
            "integrationsdata": self.validate_integration_data(),
            "titel": self.title,
            "virkning": self.create_validity()
        }

        # Add all user specified properties
        if self.description:
            properties["beskrivelse"] = self.description

        if self.scope:
            properties["omfang"] = self.scope

        if self.example:
            properties["eksempel"] = self.example

        attributter = {
            "klasseegenskaber": [properties]
        }

        relationer = {
            "ansvarlig": [
                {
                    "objekttype": "organisation",
                    "uuid": self.organisation_uuid,
                    "virkning": self.create_validity()
                }
            ],
            "facet": [
                {
                    "objekttype": "facet",
                    "uuid": self.facet_uuid,
                    "virkning": self.create_validity()
                }
            ]
        }

        tilstande = {
            "klassepubliceret": [
                {
                    "publiceret": "Publiceret",
                    "virkning": self.create_validity()
                }
            ]
        }

        return {
            "attributter": attributter,
            "relationer": relationer,
            "tilstande": tilstande
        }


class Itsystem(Base):
    """
    The Itsystem class provides functionality for collecting
    user defined itsystems for import.

    Employees can have a relation to an itsystem.

    TODO:
        Prepare for upcoming changes to "Itsystem" which will allow
        to attach a username tied to the system
        (which differs from user_key)

    """

    def __init__(self, system_name, user_key=None):
        super().__init__()

        self.system_name = system_name
        self.user_key = (user_key or system_name)

        self.organisation_uuid = None

    def build(self):
        properties = {
            "brugervendtnoegle": self.user_key,
            "itsystemnavn": self.system_name,
            "integrationsdata": self.validate_integration_data(),
            "virkning": self.create_validity()
        }

        attributter = {
            "itsystemegenskaber": [properties]
        }

        relationer = {
            "tilhoerer": [
                {
                    "uuid": self.organisation_uuid,
                    "virkning": self.create_validity()
                }
            ]
        }

        tilstande = {
            "itsystemgyldighed": [
                {
                    "gyldighed": "Aktiv",
                    "virkning": self.create_validity()
                }
            ]
        }

        return {
            "attributter": attributter,
            "relationer": relationer,
            "tilstande": tilstande
        }


class Klassifikation(Base):

    def __init__(self, user_key, parent_name, description,
                 date_from=None, date_to=None):

        # Init parent
        super().__init__()

        self.user_key = user_key
        self.description = description
        self.parent_name = parent_name

        self.organisation_uuid = None
        self.date_from = (date_from or "1900-01-01")
        self.date_to = (date_to or "infinity")

    def build(self):
        properties = {
            "brugervendtnoegle": self.user_key,
            "beskrivelse": self.description,
            "kaldenavn": self.parent_name,
            "integrationsdata": self.validate_integration_data(),
            "virkning": self.create_validity()
        }

        attributter = {
            "klassifikationegenskaber": [properties]
        }

        relationer = {
            "ansvarlig": [
                {
                    "objekttype": "organisation",
                    "uuid": self.organisation_uuid,
                    "virkning": self.create_validity()
                }
            ],
            "ejer": [
                {
                    "objekttype": "organisation",
                    "uuid": self.organisation_uuid,
                    "virkning": self.create_validity()
                }
            ]
        }

        tilstande = {
            "klassifikationpubliceret": [
                {
                    "publiceret": "Publiceret",
                    "virkning": self.create_validity()
                }
            ]
        }

        return {
            "attributter": attributter,
            "relationer": relationer,
            "tilstande": tilstande
        }


class Organisation(Base):
    """
    The Organisation class functions as a wrapper for all the sub classes.
    It also provides similar functionalitiy for importing/creating
    the parent organisation.

    Organisation
     \
      - Facet
      - Klasse
      - Itsystem
      - Organisation Unit
      - Employee

    TODO:
        Add functionality to import / inherit UUID's for existing data
        in order to use this utility for additive purposes.

        (Currently only import "from scratch" is supported)

    :param name:
    :param user_key:
    :param municipality_code:
        3-digit municipality code (str)
        In the current context the actual value is not in use.

    :param uuid:
        Imported UUID from the source (str)
        (Optional: uuid is either imported or created on insert)

    :param date_from:
        Start date e.g. "1900-01-01" (str)

    :param date_to:
        End date e.g. "1900-01-01" (str)

    :param create_defaults:
        Create default facet and klasse types (bool)

    """

    def __init__(self, name, user_key=None, municipality_code=999,
                 uuid=None, date_from=None, date_to=None):
        super().__init__()

        self.uuid = uuid
        self.name = name
        self.user_key = (user_key or name)
        self.municipality_code = str(municipality_code)

        self.date_from = (date_from or "1900-01-01")
        self.date_to = (date_to or "infinity")

    def build(self):
        properties = {
            "brugervendtnoegle": self.user_key,
            "organisationsnavn": self.name,
            "integrationsdata": self.validate_integration_data(),
            "virkning": self.create_validity()
        }

        attributter = {
            "organisationegenskaber": [
                properties
            ]
        }

        # Create urn value
        urn_municipality_code = "urn:dk:kommune:{code}".format(
            code=self.municipality_code
        )

        relationer = {
            "myndighed": [
                {
                    "urn": urn_municipality_code,
                    "virkning": self.create_validity()
                }
            ]
        }

        tilstande = {
            "organisationgyldighed": [
                {
                    "gyldighed": "Aktiv",
                    "virkning": self.create_validity()
                }
            ]
        }

        return {
            "attributter": attributter,
            "relationer": relationer,
            "tilstande": tilstande
        }