import uuid


def _virkning(dato=None):
    if dato is None:
        dato = "1910-01-01 00:00:00"
    virkning = {
        "from": dato,
        "to": "infinity",
        "aktoerref": "ddc99abd-c1b0-48c2-aef7-74fea841adae",
        "aktoertypekode": "Bruger",
    }
    return virkning


def lora_facet(bvn, org_uuid, description=None):

    attributter = {
        "facetegenskaber": [{"brugervendtnoegle": bvn, "virkning": _virkning(),}]
    }
    if description:
        attributter["facetegenskaber"][0]["beskrivelse"] = description

    relationer = {
        "ansvarlig": [
            {"objekttype": "organisation", "uuid": org_uuid, "virkning": _virkning()}
        ]
    }
    tilstande = {
        "facetpubliceret": [{"publiceret": "Publiceret", "virkning": _virkning()}]
    }
    facet = {
        "attributter": attributter,
        "relationer": relationer,
        "tilstande": tilstande,
    }
    return facet


def lora_klasse(
    bvn,
    title,
    facet_uuid,
    org_uuid,
    org_unit_uuid=None,
    description=None,
    dato=None,
    scope=None,
    parent_uuid=None,
):
    # NOTE: This is used from SD, and should be split out as a library?
    attributter = {
        "klasseegenskaber": [
            {
                "brugervendtnoegle": bvn,
                "titel": title,
                "virkning": _virkning(dato),
            }
        ]
    }
    if description:
        attributter["klasseegenskaber"][0]["beskrivelse"] = description
    if scope:
        attributter["klasseegenskaber"][0]["omfang"] = scope
    tilstande = {
        "klassepubliceret": [
            {"publiceret": "Publiceret", "virkning": _virkning(dato)}
        ]
    }
    relationer = {
        "facet": [
            {"uuid": facet_uuid, "virkning": _virkning(dato), "objekttype": "Facet"}
        ],
        "overordnetklasse": [
            {"uuid": parent_uuid, "virkning": _virkning(dato), "objekttype": "Klasse"}
        ],
        "ansvarlig": [
            {
                "uuid": org_uuid,
                "virkning": _virkning(dato),
                "objekttype": "Organisation",
            }
        ],
        "ejer": [
            {
                "uuid": org_unit_uuid,
                "virkning": _virkning(dato),
                "objekttype": "OrganisationEnhed",
            }
        ],
    }
    if parent_uuid is None:
        del relationer["overordnetklasse"]
    if org_unit_uuid is None:
        del relationer["ejer"]
    klasse = {
        "attributter": attributter,
        "relationer": relationer,
        "tilstande": tilstande,
    }

    return klasse
