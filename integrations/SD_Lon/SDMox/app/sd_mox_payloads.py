import datetime
import collections


boilerplate = {
    "@xmlns": "urn:oio:sagdok:organisation:organisationenhed:2.0.0",
    "@xmlns:oio": "urn:oio:definitions:1.0.0",
    "@xmlns:sd": "urn:oio:sagdok:3.0.0",
    "@xmlns:orgfaelles": "urn:oio:sagdok:organisation:2.0.0",
    "@xmlns:silkdata": "urn:oio:silkdata:1.0.0",
    "@xmlns:cvr": "http://rep.oio.dk/cvr.dk/xml/schemas/2005/03/22/",
    "@xmlns:dkcc1": "http://rep.oio.dk/ebxml/xml/schemas/dkcc/2003/02/13/",
    "@xmlns:dkcc2": "http://rep.oio.dk/ebxml/xml/schemas/dkcc/2005/03/15/",
    "@xmlns:itst1": "http://rep.oio.dk/itst.dk/xml/schemas/2005/06/24/",
    "@xmlns:sd20070301": "http://rep.oio.dk/sd.dk/xml.schema/20070301/",
    "@xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
    "@xsi:schemaLocation": "urn:oio:sagdok:organisation:organisationenhed:2.0.0 OrganisationEnhedRegistrering.xsd urn:oio:silkdata:1.0.0 SDObjekt.xsd"  # noqa
}


def sd_virkning(from_time, to_time=None):
    from_string = datetime.datetime.strftime(
        from_time, '%Y-%m-%dT%H:%M:%S.00'
    )

    validity = {
        "sd:FraTidspunkt": {
            "sd:TidsstempelDatoTid": from_string
        }
    }

    if to_time is not None:
        to_string = datetime.datetime.strftime(
            to_time, '%Y-%m-%dT%H:%M:%S.00'
        )
        validity['sd:TilTidspunkt'] = {
            "sd:TidsstempelDatoTid": to_string
        }
    return validity


def create_objekt_id(unit_uuid):
    objekt_id = {
        'sd:UUIDIdentifikator': unit_uuid,
        'sd:IdentifikatorType': 'OrganisationEnhed'
    }
    return objekt_id


def relations_ret(virkning, pnummer=None, phone=None, adresse=None):
    # TODO: Handle the difference between not updating and blanking a value.
    # DONE: Vi medsender altid alle felter, så mangler ingen, og blanke er blanke.

    locations = {}
    if adresse is not None:
        adresse['sd:Virkning'] = virkning,
        locations['silkdata:DanskAdresse'] = adresse

    if pnummer is not None:
        locations['silkdata:ProduktionEnhed'] = {
            'sd:Virkning': virkning,
            'silkdata:ProduktionEnhedIdentifikator': pnummer
        }
    if phone is not None:
        locations['silkdata:Kontakt'] = {
            'sd:Virkning': virkning,
            'silkdata:LokalTelefonnummerIdentifikator': phone
        }

    relations_liste = {
        'sd:LokalUdvidelse': {
            'silkdata:Lokation': locations
        }
    }
    return relations_liste


def _create_attribut_items(virkning, attributes):
    attribute_items = []
    for key, value in sorted(attributes.items()):
        attribute_items.append(
            {
                'sd:Virkning': virkning,
                'silkdata:AttributNavn': key,
                'silkdata:AttributVaerdi': value
            }
        )
    return attribute_items


def attributes_ret(virkning, funktionskode=None,
                   skolekode=None, tidsregistrering=None, unit_name=None):
    attributes = {}
    if funktionskode is not None:
        attributes["FunktionKode"] = funktionskode
    if skolekode is not None:
        attributes["SkoleKode"] = skolekode
    if tidsregistrering is not None:
        attributes["Tidsregistrering"] = tidsregistrering
    integration_items = _create_attribut_items(virkning, attributes)
    attribut_liste = {
        "sd:LokalUdvidelse": {
            "silkdata:Integration": integration_items
        }
    }
    if unit_name:
        attribut_liste['Egenskab'] = {
            "sd:EnhedNavn": unit_name,
            "sd:Virkning": virkning
        }
    return attribut_liste


def flyt_xml_dict(unit_uuid=None, unit_name=None, virk_from=None, virk_to=None,
                  parent_unit_uuid=None, **kwargs):
    reg = {'RegistreringBesked': collections.OrderedDict({
        '@xmlns': 'urn:oio:sagdok:organisation:organisationenhed:2.0.0',
        '@xmlns:cvr': 'http://rep.oio.dk/cvr.dk/xml/schemas/2005/03/22/',
        '@xmlns:dkcc1': 'http://rep.oio.dk/ebxml/xml/schemas/dkcc/2003/02/13/',
        '@xmlns:dkcc2': 'http://rep.oio.dk/ebxml/xml/schemas/dkcc/2005/03/15/',
        '@xmlns:itst1': 'http://rep.oio.dk/itst.dk/xml/schemas/2005/06/24/',
        '@xmlns:oio': 'urn:oio:definitions:1.0.0',
        '@xmlns:orgfaelles': 'urn:oio:sagdok:organisation:2.0.0',
        '@xmlns:sd': 'urn:oio:sagdok:3.0.0',
        '@xmlns:sd20070301': 'http://rep.oio.dk/sd.dk/xml.schema/20070301/',
        '@xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance',
        '@xsi:schemaLocation': 'urn:oio:sagdok:organisation:organisationenhed:2.0.0 '
        'OrganisationEnhedRegistrering.xsd',
    })}

    rb = reg['RegistreringBesked']
    rb["ObjektID"] = create_objekt_id(unit_uuid)

    rg = rb["Registrering"] = collections.OrderedDict()
    rg['sd:FraTidspunkt'] = {'sd:TidsstempelDatoTid': virk_from},
    rg['sd:LivscyklusKode'] = 'Flyttet'
    rg['sd:BrugerRef'] = {
        'sd:IdentifikatorType': 'AD',
        'sd:UUIDIdentifikator': '3bb66b0d-132d-4b98-a903-ea29f6552d53'
    }

    rg['AttributListe'] = {
        'Egenskab': {
            'sd:EnhedNavn': unit_name,
            'sd:Virkning': {
                'sd:FraTidspunkt': {'sd:TidsstempelDatoTid': virk_from},
                'sd:TilTidspunkt': {'sd:TidsstempelDatoTid': virk_to}
            }
        },
        'sd:LokalUdvidelse': None
    }

    rg['TilstandListe'] = None

    rg['RelationListe'] = {
        'sd:LokalUdvidelse': None,
        'sd:Overordnet': {
            'sd:ReferenceID': {'sd:UUIDIdentifikator': parent_unit_uuid},
            'sd:Virkning': {
                'sd:FraTidspunkt': {'sd:TidsstempelDatoTid': virk_from},
                'sd:TilTidspunkt': {'sd:TidsstempelDatoTid': virk_to}
            }
        }
    }

    return reg


def import_xml_dict(unit_uuid=None, unit_name=None, unit_code=None, unit_level=None,
                    virk_from=None, virk_to=None, parent_unit_uuid=None, **kwargs):
    reg = collections.OrderedDict({'RegistreringBesked': collections.OrderedDict({
        '@xmlns': 'urn:oio:sagdok:organisation:organisationenhed:2.0.0',
        '@xmlns:cvr': 'http://rep.oio.dk/cvr.dk/xml/schemas/2005/03/22/',
        '@xmlns:dkcc1': 'http://rep.oio.dk/ebxml/xml/schemas/dkcc/2003/02/13/',
        '@xmlns:dkcc2': 'http://rep.oio.dk/ebxml/xml/schemas/dkcc/2005/03/15/',
        '@xmlns:itst1': 'http://rep.oio.dk/itst.dk/xml/schemas/2005/06/24/',
        '@xmlns:oio': 'urn:oio:definitions:1.0.0',
        '@xmlns:orgfaelles': 'urn:oio:sagdok:organisation:2.0.0',
        '@xmlns:sd': 'urn:oio:sagdok:3.0.0',
        '@xmlns:sd20070301': 'http://rep.oio.dk/sd.dk/xml.schema/20070301/',
        '@xmlns:silkdata': 'urn:oio:silkdata:1.0.0',
        '@xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance',
        '@xsi:schemaLocation': 'urn:oio:sagdok:organisation:organisationenhed:2.0.0 '
                               'OrganisationEnhedRegistrering.xsd '
                               'urn:oio:silkdata:1.0.0 '
                               'SDObjekt.xsd'})})

    rb = reg['RegistreringBesked']
    rb["ObjektID"] = create_objekt_id(unit_uuid)

    rg = rb["Registrering"] = collections.OrderedDict()

    rg['sd:FraTidspunkt'] = {'sd:TidsstempelDatoTid': virk_from},
    rg['sd:LivscyklusKode'] = 'Opstaaet'
    rg['sd:BrugerRef'] = {
        'sd:IdentifikatorType': 'ILTEST',
        'sd:UUIDIdentifikator': '3bb66b0d-132d-4b98-a903-ea29f6552mmm'
    }

    ra = rg['AttributListe'] = collections.OrderedDict()

    ra['Egenskab'] = collections.OrderedDict({
        'sd:EnhedNavn': unit_name,
        'sd:Virkning': {
            'sd:FraTidspunkt': {'sd:TidsstempelDatoTid': virk_from},
            'sd:TilTidspunkt': {'sd:TidsstempelDatoTid': virk_to}
        }
    })
    ra['sd:LokalUdvidelse'] = {
        'silkdata:Integration': [{
            'sd:Virkning': {
                'sd:FraTidspunkt': {'sd:TidsstempelDatoTid': virk_from},
                'sd:TilTidspunkt': {'sd:TidsstempelDatoTid': virk_to}
            },
            'silkdata:AttributNavn': 'EnhedKode',
            'silkdata:AttributVaerdi': unit_code
        }, {
            'sd:Virkning': {
                'sd:FraTidspunkt': {'sd:TidsstempelDatoTid': virk_from},
                'sd:TilTidspunkt': {'sd:TidsstempelDatoTid': virk_to}
            },
            'silkdata:AttributNavn': 'Niveau',
            'silkdata:AttributVaerdi': unit_level
        }],
    }

    rg['TilstandListe'] = {
        'orgfaelles:Gyldighed': {
            'orgfaelles:GyldighedStatusKode': 'Aktiv',
            'sd:Virkning': {
                'sd:FraTidspunkt': {'sd:TidsstempelDatoTid': virk_from},
                'sd:TilTidspunkt': {'sd:TidsstempelDatoTid': virk_to}
            }
        }
    }

    rg['RelationListe'] = {
        'sd:LokalUdvidelse': None,
        'sd:Overordnet': {
            'sd:ReferenceID': {'sd:UUIDIdentifikator': parent_unit_uuid},
            'sd:Virkning': {
                'sd:FraTidspunkt': {'sd:TidsstempelDatoTid': virk_from},
                'sd:TilTidspunkt': {'sd:TidsstempelDatoTid': virk_to}
            }
        }
    }
    return reg


def attributes_flyt(virkning, name):
    return {
        "Egenskab": {
            "sd:EnhedNavn": name,
            "sd:Virkning": virkning
        }
    }


def create_registrering(virkning, registry_type):
    if registry_type not in ('Rettet', 'Opstaaet'):
        raise RuntimeError(
            "Integration, SD-løn: ukendt registreringstype: %s" % (
                registry_type,
            )
        )
    registrering = {
        'sd:LivscyklusKode': registry_type,
        'TilstandListe': {
            "orgfaelles:Gyldighed": {
                "sd:Virkning": virkning,
                "orgfaelles:GyldighedStatusKode": "Aktiv"
            }
        }
    }
    registrering.update(sd_virkning(datetime.datetime.now()))
    return registrering
