# -- coding: utf-8 --


class Base(object):
    """
    Base class for all MOX type objects

    :param integration_data:
    An arbitrary json string with relationships between
    user defined identifiers and the actual object uuids (str)

    This is an experimentation functionality
    first and foremost to facilitate multiple imports
    to avoid creating duplicate objects.

    :param date_from:
    Start date e.g. "1900-01-01" (str)

    :param date_to:
    End date e.g. "1900-01-01" (str)
    If left empty, it defaults to "infinity"
    """
    def __init__(self):

        self.date_from = "1900-01-01"
        self.date_to = "infinity"
        self.integration_data = {}

    def validate_integration_data(self):
        """
        Apply integration data to the payload.

        :return: Integration data (dict)
        """

        if not isinstance(self.integration_data, dict):
            raise TypeError("Integration data must be passed as dict")

        if "integration_data" not in self.integration_data:
            return str()

        return self.integration_data['integration_data']

    def create_validity(self):
        """
        Create validity key/value pair from date_from and date_to.

        :return: Validity (dict)
        """

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
    Facet type - parent of klasse type objects.

    In the current implementation of the os2mo application
    no other types than default values should be needed.

    :param uuid:
    The object uuid (str/uuid)
    This can be set to forcefully import the object with its current uuid.
    If the object already exists in the database, it is updated/overridden.

    :param integration_data:
    An arbitrary json string with relationships between
    user defined identifiers and the actual object uuids (str)

    :param date_from:
    Start date e.g. "1900-01-01" (str)

    :param date_to:
    End date e.g. "1900-01-01" (str)
    If left empty, it defaults to "infinity"
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
        """
        Build a MOX POST data payload

        :return: POST data payload (dict)
        """

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
    The Klasse type provides functionality for collecting
    user defined klasse meta objects for import.

    In the current implementation of the os2mo application
    2 Klasse objects must be created:

        "user_key: "Telefon"
        "user_key: "AdressePost"


    :param uuid:
    The object uuid (str/uuid)
    This can be set to forcefully import the object with its current uuid.
    If the object already exists in the database, it is updated/overridden.

    :param title:
    The frontend application expects these two klasse objects to exist.
    These provide functionality for 2 required input fields.

    :param user_key:
    Optionally set a specific user_key (str)
    This is mainly usefull as an internal reference.

    :param scope:
    Scope type value for validation and field generation (str)
    Must be one of the following values:

        DAR, EAN, EMAIL, PHONE, PNUMBER, TEXT, WWW

    :param example:
    An example value displaying the format of the parameter.
    This is currently not displayed in the frontend application.

    {
        ...
        "example": "<UUID>"
    }

    :param description:
    A description paragraph (str)
    This is currently not displayed in the frontend application.

    {
        ...
        "description": "Please write something beautiful"
    }

    :param facet_type_ref:
    Reference to the parent Facet type (str)

    :param date_from:
    Start date e.g. "1900-01-01" (str)

    :param date_to:
    End date e.g. "1900-01-01" (str)
    If left empty, it defaults to "infinity"
    """

    def __init__(self, facet_type_ref, user_key, description=None,
                 example=None, scope=None, title=None, uuid=None,
                 date_from=None, date_to=None):
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
        """
        Build a MOX POST data payload

        :return: POST data payload (dict)
        """

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
    Itsystem class for creating it systems which belong to the organisation.
    Employees can be tied to an it system with a user name.

    :param system_name:
    Name of the it system (str)
    Examples: "Main computer", "Employee database" etc.

    :param user_key:
    Username or entry point to the associated it system (str)
    """

    def __init__(self, system_name, user_key=None):
        super().__init__()

        self.system_name = system_name
        self.user_key = (user_key or system_name)

        self.organisation_uuid = None

    def build(self):
        """
        Build a MOX POST data payload

        :return: POST data payload (dict)
        """
        properties = {
            "brugervendtnoegle": self.user_key,
            "integrationsdata": self.validate_integration_data(),
            "itsystemnavn": self.system_name,
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
    """
    The Klassification object is the parent of all Facet types
    within an organisation.

    It is automatically created along with the organisation.

    :param user_key:
    Name of the parent organisation (str)

    :param parent_name:
    Name of the parent organisation or alias (str)

    :param description:
    A description paragraph (str)

    :param date_from:
    Start date e.g. "1900-01-01" (str)

    :param date_to:
    End date e.g. "1900-01-01" (str)
    If left empty, it defaults to "infinity"
    """

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
        """
        Apply class specific attributes before calling
        the underlying _build method to create the POST data payload.

        :return: POST data payload (dict)
        """
        properties = {
            "brugervendtnoegle": self.user_key,
            "integrationsdata": self.validate_integration_data(),
            "beskrivelse": self.description,
            "kaldenavn": self.parent_name,
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
    The Organisation object is the top level parent
    for all the underlying Facet, Klasse, Organisation Unit and Employee objects.

    Organisation
     \
      - Facet
      - Klasse
      - Itsystem
      - Organisation Unit
      - Employee

    :param name:
    Canonical name of the organisation (str)

    :param user_key:
    Optionally reference a UUID or logical name for the organisation (str)

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
        """
        Apply class specific attributes before calling
        the underlying _build method to create the POST data payload.

        :return: POST data payload (dict)
        """
        properties = {
            "brugervendtnoegle": self.user_key,
            "integrationsdata": self.validate_integration_data(),
            "organisationsnavn": self.name,
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
