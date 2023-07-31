import logging
from abc import ABC
from abc import abstractmethod
from enum import Enum

import requests
from gql import gql
from os2mo_helpers.mora_helpers import MoraHelper
from ra_utils.headers import TokenSettings
from ra_utils.job_settings import JobSettings
from raclients.graph.client import GraphQLClient

logger = logging.getLogger(__name__)


class Settings(JobSettings):
    integrations_kle_xlsx_file_path: str
    integrations_kle_xlsx_org_unit_levels: list | None = None


class Aspects(Enum):
    Indsigt = 1
    Udfoerende = 2
    Ansvarlig = 3


# Maps between the enum and scopes on the classes from the aspect facet
ASPECT_MAP = {
    "INDSIGT": Aspects.Indsigt,
    "UDFOERENDE": Aspects.Udfoerende,
    "ANSVARLIG": Aspects.Ansvarlig,
}


class KLEAnnotationIntegration(ABC):
    """Import and export of KLE annotation from or to an external source."""

    # XXX: This uses a simple inheritance based pattern. We might want to use
    # something like a Strategy here. However, maybe YAGNI.

    def __init__(self):

        self.settings = Settings()
        self.settings.start_logging_based_on_settings()

        self.mora_base = self.settings.mora_base
        self.mora_session = self._get_mora_session()
        self.helper = MoraHelper(hostname=self.mora_base)
        self.org_uuid = self.helper.read_organisation()

        kle_classes = self.get_kle_classes_from_mo()
        self.kle_uuid_map = {item["user_key"]: item["uuid"] for item in kle_classes}

        aspect_classes = self.get_aspect_classes_from_mo()
        self.aspect_map = {
            ASPECT_MAP[clazz["scope"]]: clazz["uuid"] for clazz in aspect_classes
        }
        self.gql_client = GraphQLClient(
            url=f"{self.mora_base}/graphql/v7",
            client_id=self.settings.client_id,
            client_secret=self.settings.client_secret,
            auth_realm=self.settings.auth_realm,
            auth_server=self.settings.auth_server,
            sync=True,
        )

    def _get_mora_session(self) -> requests.Session:
        s = requests.Session()
        session_headers = TokenSettings().get_headers()

        if session_headers:
            s.headers.update(session_headers)
        s.verify = False
        return s

    def _get_mo_org_uuid(self) -> str:
        """
        Get the UUID of the organisation configured in OS2mo
        :return:
        """
        logger.info("Fetching Organisation UUID from OS2mo")
        r = self.mora_session.get("{}/service/o/".format(self.mora_base))
        r.raise_for_status()
        return r.json()[0]["uuid"]

    def get_kle_classes_from_mo(self) -> list:
        """Get all of the kle_number 'klasse' objects from OS2mo"""
        logger.info("Fetching KLE numbers from OS2mo")
        kle_numbers, _ = self.helper.read_classes_in_facet("kle_number")
        return kle_numbers

    def get_aspect_classes_from_mo(self) -> list:
        """Get all of the kle_aspect 'klasse' objects from OS2mo"""
        logger.info("Fetching KLE aspect classes from OS2mo")
        kle_aspects, _ = self.helper.read_classes_in_facet("kle_aspect")
        return kle_aspects

    def get_all_org_units_from_mo(self) -> list:
        """Get a list of all units from OS2mo"""
        logger.info("Fetching all org units from OS2mo")
        query = gql(
            """
            query OrgUnitQuery {
              org_units {
                objects {
                  current {
                    uuid
                    org_unit_level {
                      name
                    }
                    name
                  }
                }
              }
            }
            """
        )

        r = self.gql_client.execute(query)

        units = [o["current"] for o in r["org_units"]["objects"]]
        # filter by org_unit_level if configured in settings
        if org_unit_levels := self.settings.integrations_kle_xlsx_org_unit_levels:
            units = [
                o for o in units if o["org_unit_level"].get("name") in org_unit_levels
            ]
        logger.info("Found {} units".format(len(units)))
        return units

    def post_payloads_to_mo(self, payloads: list):
        """Submit a list of details payloads to OS2mo"""
        logger.info("Posting payloads to OS2mo ")
        url = "{}/service/details/create".format(self.mora_base)

        r = self.mora_session.post(url, json=payloads, params={"force": 1})
        r.raise_for_status()

    @abstractmethod
    def run(self):
        """Implement this, normally to execute import or export."""
        pass
