# type: ignore
from io import StringIO

from requests import Session
from requests.auth import HTTPBasicAuth
from zeep import Client
from zeep.cache import SqliteCache
from zeep.proxy import ServiceProxy
from zeep.transports import Transport


WSDL_TOP = """<?xml version="1.0"?>
<definitions
    xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
    name="SDLoen"
    targetNamespace="www.sd.dk/sdws/"
>
    <!--
        Organization Endpoints
    -->
    <!-- GetDepartment -->
    <wsdl:import
        namespace="www.sd.dk/sdws/GetDepartment20080201"
        location="https://service.sd.dk/sdws/GetDepartment20080201WSDL"
    />
    <wsdl:import
        namespace="www.sd.dk/sdws/GetDepartment20111201"
        location="https://service.sd.dk/sdws/xml/schema/sd.dk/xml.wsdl/20111201/GetDepartment20111201.wsdl"
    />

    <!-- GetDepartmentParent -->
    <wsdl:import
        namespace="www.sd.dk/sdws/GetDepartmentParent20190701"
        location="https://service.sd.dk/sdws/xml/schema/sd.dk/xml.wsdl/20190701/GetDepartmentParent20190701.wsdl"
    />

    <!-- GetInstitution -->
    <wsdl:import
        namespace="www.sd.dk/sdws/GetInstitution20080201"
        location="https://service.sd.dk/sdws/GetInstitution20080201WSDL"
    />
    <wsdl:import
        namespace="www.sd.dk/sdws/GetInstitution20111201"
        location="https://service.sd.dk/sdws/xml/schema/sd.dk/xml.wsdl/20111201/GetInstitution20111201.wsdl"
    />

    <!-- GetOrganization -->
    <wsdl:import
        namespace="www.sd.dk/sdws/GetOrganization"
        location="https://service.sd.dk/sdws/GetOrganizationWSDL"
    />
    <wsdl:import
        namespace="www.sd.dk/sdws/GetOrganization20080201"
        location="https://service.sd.dk/sdws/GetOrganization20080201WSDL"
    />
    <wsdl:import
        namespace="www.sd.dk/sdws/GetOrganization20111201"
        location="https://service.sd.dk/sdws/xml/schema/sd.dk/xml.wsdl/20111201/GetOrganization20111201.wsdl"
    />

    <!--
        Person and employment Endpoints
    -->
    <!-- GetEmployment -->
    <wsdl:import
        namespace="www.sd.dk/sdws/GetEmployment20070401"
        location="https://service.sd.dk/sdws/GetEmployment20070401WSDL"
    />
    <wsdl:import
        namespace="www.sd.dk/sdws/GetEmployment20111201"
        location="https://service.sd.dk/sdws/xml/schema/sd.dk/xml.wsdl/20111201/GetEmployment20111201.wsdl"
    />

    <!-- GetEmploymentChanged -->
    <wsdl:import
        namespace="www.sd.dk/sdws/GetEmploymentChanged20070401"
        location="https://service.sd.dk/sdws/GetEmploymentChanged20070401WSDL"
    />
    <wsdl:import
        namespace="www.sd.dk/sdws/GetEmploymentChanged20111201"
        location="https://service.sd.dk/sdws/xml/schema/sd.dk/xml.wsdl/20111201/GetEmploymentChanged20111201.wsdl"
    />

    <!-- GetEmploymentAtDateChanged -->
    <wsdl:import
        namespace="www.sd.dk/sdws/GetEmploymentChangedAtDate20070401"
        location="https://service.sd.dk/sdws/GetEmploymentChangedAtDate20070401WSDL"
    />
    <wsdl:import
        namespace="www.sd.dk/sdws/GetEmploymentChangedAtDate20111201"
        location="https://service.sd.dk/sdws/xml/schema/sd.dk/xml.wsdl/20111201/GetEmploymentChangedAtDate20111201.wsdl"
    />

    <!-- GetPerson -->
    <wsdl:import
        namespace="www.sd.dk/sdws/GetPerson"
        location="https://service.sd.dk/sdws/GetPersonWSDL"
    />
    <wsdl:import
        namespace="www.sd.dk/sdws/GetPerson20111201"
        location="https://service.sd.dk/sdws/xml/schema/sd.dk/xml.wsdl/20111201/GetPerson20111201.wsdl"
    />

    <!-- GetPersonChangedAtDate -->
    <wsdl:import
        namespace="www.sd.dk/sdws/GetPersonChangedAtDate"
        location="https://service.sd.dk/sdws/GetPersonChangedAtDateWSDL"
    />
    <wsdl:import
        namespace="www.sd.dk/sdws/GetPersonChangedAtDate20111201"
        location="https://service.sd.dk/sdws/xml/schema/sd.dk/xml.wsdl/20111201/GetPersonChangedAtDate20111201.wsdl"
    />

    <!--
        Profession Endpoints
    -->
    <!-- GetProfession -->
    <wsdl:import
        namespace="www.sd.dk/sdws/GetProfession"
        location="https://service.sd.dk/sdws/GetProfessionWSDL"
    />
    <wsdl:import
        namespace="www.sd.dk/sdws/GetProfession20080201"
        location="https://service.sd.dk/sdws/GetProfession20080201WSDL"
    />
</definitions>
"""


class SDSoapClient:
    def __init__(self, username: str, password: str):
        session = Session()
        session.auth = HTTPBasicAuth(username, password)
        wsdl_top = StringIO(WSDL_TOP)
        client = Client(
            wsdl_top, transport=Transport(session=session, cache=SqliteCache())
        )
        definitions = list(client.wsdl._definitions.values())

        for definition in definitions:
            for service_name, service in definition.services.items():
                assert len(service.ports) == 1
                ((port_name, port),) = service.ports.items()

                service = ServiceProxy(client, port.binding, **port.binding_options)
                assert len(service._operations) == 1

                ((operation_name, operation),) = service._operations.items()
                assert hasattr(self, port_name) is False
                setattr(self, port_name, operation)


if __name__ == "__main__":
    import click

    @click.command()
    @click.option(
        "--username",
        required=True,
        help="SD username",
    )
    @click.option(
        "--password",
        required=True,
        help="SD password",
        prompt=True,
        hide_input=True,
    )
    def main(username: str, password: str):
        soap_client = SDSoapClient(username, password)
        result = soap_client.GetInstitution20111201(
            InstitutionIdentifier="BR",
            AdministrationIndicator=False,
            ContactInformationIndicator=False,
            PostalAddressIndicator=False,
            ProductionUnitIndicator=False,
            UUIDIndicator=True,
        )
        print(result)

    main()
