# SPDX-FileCopyrightText: 2023 Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
def _virkning(dato="1910-01-01 00:00:00"):
    virkning = {
        "from": dato,
        "to": "infinity",
        "aktoerref": "ddc99abd-c1b0-48c2-aef7-74fea841adae",
        "aktoertypekode": "Bruger",
    }
    return virkning


def lora_facet(bvn, org="ddc99abd-c1b0-48c2-aef7-74fea841adae"):
    attributter = {
        "facetegenskaber": [
            {
                "brugervendtnoegle": bvn,
                "virkning": _virkning(),
            }
        ]
    }
    tilstande = {
        "facetpubliceret": [{"publiceret": "Publiceret", "virkning": _virkning()}]
    }
    relationer = {
        "ansvarlig": [
            {"objekttype": "organisation", "uuid": org, "virkning": _virkning()}
        ]
    }
    facet = {
        "attributter": attributter,
        "tilstande": tilstande,
        "relationer": relationer,
    }
    return facet


def lora_klasse(
    brugervendtnoegle,
    beskrivelse,
    titel,
    dato,
    facet,
    ansvarlig,
    omfang=None,
    overklasse=None,
):
    attributter = {
        "klasseegenskaber": [
            {
                "brugervendtnoegle": brugervendtnoegle,
                "beskrivelse": beskrivelse,
                "titel": titel,
                "virkning": _virkning(dato),
            }
        ]
    }
    tilstande = {
        "klassepubliceret": [{"publiceret": "Publiceret", "virkning": _virkning(dato)}]
    }
    relationer = {
        "facet": [{"uuid": facet, "virkning": _virkning(dato), "objekttype": "Facet"}],
        "overordnetklasse": [{"virkning": _virkning(dato), "objekttype": "Klasse"}],
        "ansvarlig": [
            {
                "uuid": ansvarlig,
                "virkning": _virkning(dato),
                "objekttype": "Organisation",
            }
        ],
    }
    klasse = {
        "attributter": attributter,
        "tilstande": tilstande,
        "relationer": relationer,
    }
    if overklasse is not None:
        klasse["relationer"]["overordnetklasse"][0]["uuid"] = overklasse
    else:
        del klasse["relationer"]["overordnetklasse"]

    if omfang:
        attributter["klasseegenskaber"][0]["omfang"] = omfang

    return klasse
