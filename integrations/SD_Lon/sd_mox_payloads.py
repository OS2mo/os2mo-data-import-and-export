import datetime

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
        "@xsi:schemaLocation": "urn:oio:sagdok:organisation:organisationenhed:2.0.0 OrganisationEnhedRegistrering.xsd urn:oio:silkdata:1.0.0 SDObjekt.xsd"
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

