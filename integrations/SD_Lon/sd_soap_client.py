# type: ignore
from abc import ABC
from abc import abstractmethod
from functools import partial
from io import StringIO
from typing import Any
from typing import Callable
from typing import Iterator
from typing import Tuple

import httpx
from requests import Session
from requests.auth import HTTPBasicAuth
from zeep import AsyncClient
from zeep import Client
from zeep.cache import SqliteCache
from zeep.proxy import AsyncServiceProxy
from zeep.proxy import ServiceProxy
from zeep.transports import AsyncTransport
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


class SDSoapClientBase(ABC):
    def __init__(self, username: str, password: str):
        # Load our top-level wsdl (specifying all endpoints) into the client
        wsdl_top = StringIO(WSDL_TOP)
        client = self._create_client(wsdl_top, username, password)
        # Fetch all services from all definition endpoints
        services = self._yield_services(client)
        # Build a method-map from services (method_name, callable)
        method_map = dict(map(partial(self._service2method, client), services))
        # Export every method in the method map directly on ourself
        for method_name, cally in method_map.items():
            assert hasattr(self, method_name) is False
            setattr(self, method_name, cally)

    def _yield_services(self, client: Any) -> Iterator:
        definitions = list(client.wsdl._definitions.values())
        for definition in definitions:
            for service in definition.services.values():
                yield service

    def _service2method(self, client: Any, service: Any) -> Tuple[str, Callable]:
        assert len(service.ports) == 1
        ((port_name, port),) = service.ports.items()

        service = self._create_service_proxy(
            client, port.binding, **port.binding_options
        )
        assert len(service._operations) == 1

        ((operation_name, operation),) = service._operations.items()
        return port_name, operation

    @abstractmethod
    def _create_client(self, wsdl: StringIO, username: str, password: str) -> Any:
        raise NotImplementedError

    @abstractmethod
    def _create_service_proxy(self, client: Any, binding: Any, **kwargs) -> Any:
        raise NotImplementedError


class AsyncSDSoapClient(SDSoapClientBase):
    def _create_async_client(self, username: str, password: str):
        self.httpx_client = httpx.AsyncClient(auth=(username, password))
        return self.httpx_client

    def _create_client(
        self, wsdl: StringIO, username: str, password: str
    ) -> AsyncClient:
        httpx_client = self._create_async_client(username, password)
        # httpx_client = httpx.AsyncClient(auth=(username, password))
        client = AsyncClient(
            wsdl, transport=AsyncTransport(client=httpx_client, cache=SqliteCache())
        )
        return client

    def _create_service_proxy(self, client: AsyncClient, binding: Any, **kwargs) -> Any:
        return AsyncServiceProxy(client, binding, **kwargs)

    async def aclose(self):
        await self.httpx_client.aclose()


class SDSoapClient(SDSoapClientBase):
    def _create_client(self, wsdl: StringIO, username: str, password: str) -> Client:
        session = Session()
        session.auth = HTTPBasicAuth(username, password)
        client = Client(wsdl, transport=Transport(session=session, cache=SqliteCache()))
        return client

    def _create_service_proxy(self, client: Client, binding: Any, **kwargs) -> Any:
        return ServiceProxy(client, binding, **kwargs)


if __name__ == "__main__":
    import click
    from ra_utils.async_to_sync import async_to_sync

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
    @click.option(
        "--institution-identifier",
        required=True,
        help="SD identifier for the institution",
    )
    @async_to_sync
    async def main(username: str, password: str, institution_identifier: str):
        params = {
            "InstitutionIdentifier": institution_identifier,
            "AdministrationIndicator": False,
            "ContactInformationIndicator": False,
            "PostalAddressIndicator": False,
            "ProductionUnitIndicator": False,
            "UUIDIndicator": True,
        }
        soap_client = SDSoapClient(username, password)
        result = soap_client.GetInstitution20111201(**params)
        print(result)

        asoap_client = AsyncSDSoapClient(username, password)
        result = await asoap_client.GetInstitution20111201(**params)
        await asoap_client.aclose()
        print(result)

    main()
