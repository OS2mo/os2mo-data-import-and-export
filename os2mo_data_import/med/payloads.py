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


def lora_facet(bvn, org, description=None):

    attributter = {
        "facetegenskaber": [{"brugervendtnoegle": bvn, "virkning": _virkning(),}]
    }
    if description:
        attributter["facetegenskaber"][0]["beskrivelse"] = description

    relationer = {
        "ansvarlig": [
            {"objekttype": "organisation", "uuid": org, "virkning": _virkning()}
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
    brugervendtnoegle,
    titel,
    facet_uuid,
    org_uuid,
    description=None,
    dato=None,
    omfang=None,
    overklasse=None,
):
    attributter = {
        "klasseegenskaber": [
            {
                "brugervendtnoegle": brugervendtnoegle,
                "titel": titel,
                "virkning": _virkning(dato),
            }
        ]
    }
    if description:
        attributter["klasseegenskaber"][0]["beskrivelse"] = description
    tilstande = {
        "klassepubliceret": [{"publiceret": "Publiceret", "virkning": _virkning(dato)}]
    }
    relationer = {
        "facet": [
            {"uuid": facet_uuid, "virkning": _virkning(dato), "objekttype": "Facet"}
        ],
        "overordnetklasse": [{"virkning": _virkning(dato), "objekttype": "Klasse"}],
        "ansvarlig": [
            {
                "uuid": org_uuid,
                "virkning": _virkning(dato),
                "objekttype": "Organisation",
            }
        ],
    }
    klasse = {
        "attributter": attributter,
        "relationer": relationer,
        "tilstande": tilstande,
    }
    if overklasse is not None:
        klasse["relationer"]["overordnetklasse"][0]["uuid"] = overklasse
    else:
        del klasse["relationer"]["overordnetklasse"]

    if omfang:
        attributter["klasseegenskaber"][0]["omfang"] = omfang

    return klasse
