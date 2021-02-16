import datetime
from functools import partial
from typing import Union
from uuid import UUID

import aiohttp
from xmltodict import parse

xmlparse = partial(parse, dict_constructor=dict)


class SDAPIException(Exception):
    pass


def is_uuid(uuid: Union[str, UUID]) -> bool:
    if isinstance(uuid, UUID):
        return True

    try:
        UUID(uuid)
        return True
    except ValueError:
        return False
    except AttributeError as exp:
        print(exp)
        return False


def today() -> datetime.date:
    today = datetime.date.today()
    return today


class SDConnector:
    def __init__(
        self,
        institution_identifier: Union[str, UUID],
        sd_username: str,
        sd_password: str,
        sd_base_url: str = "https://service.sd.dk/sdws/",
    ):
        self.institution_identifier = institution_identifier
        self.auth = aiohttp.BasicAuth(sd_username, sd_password)
        self.base_url = sd_base_url

    def _enrich_with_institution(self, params: dict) -> dict:
        if is_uuid(self.institution_identifier):
            params["InstitutionUUIDIdentifier"] = str(self.institution_identifier)
        else:
            params["InstitutionIdentifier"] = self.institution_identifier
        return params

    def _enrich_with_department(
        self, params: dict, department_identifier: Union[str, UUID] = None
    ) -> dict:
        if department_identifier is None:
            return params
        if is_uuid(department_identifier):
            params["DepartmentUUIDIdentifier"] = str(department_identifier)
        else:
            params["DepartmentIdentifier"] = department_identifier
        return params

    def _enrich_with_dates(
        self,
        params: dict,
        start_date: datetime.date = None,
        end_date: datetime.date = None,
    ) -> dict:
        start_date = start_date or today()
        end_date = end_date or today()
        params.update(
            {
                "ActivationDate": start_date.strftime("%d.%m.%Y"),
                "DeactivationDate": end_date.strftime("%d.%m.%Y"),
            }
        )
        return params

    async def _send_request_xml(self, url: str, params: dict) -> str:
        """Fire a requests against SD.

        Utilizes _sd_request to fire the actual request, which in turn utilize
        _sd_lookup_cache for caching.
        """
        # logger.info("Retrieve: {}".format(url))
        # logger.debug("Params: {}".format(params))

        full_url = self.base_url + url

        async with aiohttp.ClientSession(raise_for_status=True) as session:
            async with session.get(full_url, auth=self.auth, params=params) as response:
                # We always expect SD to return UTF8 encoded XML
                assert response.headers["Content-Type"] == "text/xml;charset=UTF-8"
                return await response.text()

    async def _send_request_json(self, url: str, params: dict) -> dict:
        xml_response = await self._send_request_xml(url, params)
        dict_response = xmlparse(xml_response)
        if "Envelope" in dict_response:
            raise SDAPIException(dict_response["Envelope"])
        return dict_response

    async def getOrganization(
        self,
        start_date: datetime.date = None,
        end_date: datetime.date = None,
    ) -> dict:
        params = {
            "UUIDIndicator": "true",
        }
        params = self._enrich_with_dates(params, start_date, end_date)
        params = self._enrich_with_institution(params)
        url = "GetOrganization20111201"
        dict_response = await self._send_request_json(url, params)
        return dict_response[url]

    async def getDepartment(
        self,
        department_identifier: Union[str, UUID] = None,
        department_level_identifier: str = None,
        start_date: datetime.date = None,
        end_date: datetime.date = None,
    ) -> dict:
        params = {
            "ContactInformationIndicator": "true",
            "DepartmentNameIndicator": "true",
            "EmploymentDepartmentIndicator": "false",
            "PostalAddressIndicator": "true",
            "ProductionUnitIndicator": "true",
            "UUIDIndicator": "true",
        }
        if department_level_identifier:
            params["DepartmentLevelIdentifier"] = department_level_identifier
        params = self._enrich_with_dates(params, start_date, end_date)
        params = self._enrich_with_department(params, department_identifier)
        params = self._enrich_with_institution(params)

        url = "GetDepartment20111201"
        dict_response = await self._send_request_json(url, params)
        return dict_response[url]
