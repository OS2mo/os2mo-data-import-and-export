#
# Copyright (c) 2017-2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

"""
Default facet and klasse types needed in order for os2mo to work.
As a minimum - at least one klasse type per facet type must be created.

Additionally there are 2 magic types which MUST be present:

 - A klasse type with the user_key: Telefon
 - A klasse type with the user_key: AdressePost

The frontend GUI depends on these 2 klasse types to exist,
as they are used as a default input field type in the frontend

TODO: Create validation for missing any missing klasse types
TODO: Default types should only be added if not created by the user
"""

# Facet types are simple as they only require a user_key to be generated
# NOTE: There should not be any cases where a custom facet type is needed.


facet_defaults = [
    "org_unit_address_type",
    "employee_address_type",
    "manager_address_type",
    "address_property",
    "engagement_job_function",
    "association_job_function",
    "org_unit_type",
    "engagement_type",
    "association_type",
    "role_type",
    "leave_type",
    "manager_type",
    "responsibility",
    "manager_level"
]

klasse_defaults = [
    (
        "Enhed",
        "org_unit_type",
        {
            "description": "Dette er en organisationsenhed",
            "title": "Enhed"
        }
    ),
    (
        "LederAdressePost",
        "manager_address_type",
        {
            "example": "<UUID>",
            "scope": "DAR",
            "title": "Adresse"
        }
    ),
    (
        "LederEmail",
        "manager_address_type",
        {
            "example": "test@example.com",
            "scope": "EMAIL",
            "title": "Email"
        }
    ),
    (
        "LederTelefon",
        "manager_address_type",
        {
            "example": "20304060",
            "scope": "PHONE",
            "title": "Tlf"
        }
    ),
    (
        "LederWebadresse",
        "manager_address_type",
        {
            "example": "http://www.magenta.dk",
            "scope": "WWW",
            "title": "Webadresse"
        }
    ),
    (
        "Leder",
        "manager_type",
        {
            "title": "Leder"
        }
    ),
    (
        "Lederansvar",
        "responsibility",
        {
            "title": "Ansvar for organisationsenheden"
        }
    ),
    (
        "Lederniveau",
        "manager_level",
        {
            "title": "Niveau 90",
        }
    ),
    (
        "Ansat",
        "engagement_type",
        {
            "title": "Ansat"
        }
    ),
    (
        "Medarbejder",
        "engagement_job_function",
        {
            "title": "Generisk Medarbejder"
        }
    ),
]

# Waiting for the correct implementation of address types
pending_klasse_items = [
    (
        "AdressePost",
        "Adressetype",

        {
            "example": "<UUID>",
            "scope": "DAR",
            "title": "Adresse"
        }
    ),
    (
        "Email",
        "Adressetype",
        {
            "example": "test@example.com",
            "scope": "EMAIL",
            "title": "Email"
        }
    ),
    (
        "Telefon",
        "Adressetype",
        {
            "example": "20304060",
            "scope": "PHONE",
            "title": "Tlf"
        }
    ),
    (
        "Webadresse",
        "Adressetype",
        {
            "example": "http://www.magenta.dk",
            "scope": "WWW",
            "title": "Webadresse"
        }
    ),
    (
        "EAN",
        "Adressetype",
        {
            "example": "00112233",
            "scope": "EAN",
            "title": "EAN-nr."
        }
    ),
    (
        "PNUMBER",
        "Adressetype",
        {
            "example": "00112233",
            "scope": "PNUMBER",
            "title": "P-nr."
        }
    ),
    (
        "TEXT",
        "Adressetype",
        {
            "example": "Fritekst",
            "scope": "TEXT",
            "title": "Fritekst"
        }
    ),
]