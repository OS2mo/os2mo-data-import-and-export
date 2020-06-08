import json
import logging
import os
import pathlib
import sys
from functools import lru_cache
from logging.handlers import RotatingFileHandler

import requests
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth
from urllib3 import Retry

LOG_FILE = 'exports_opgavefordeler.log'

logger = logging.getLogger(__name__)


def init_log():
    logging.getLogger("urllib3").setLevel(logging.INFO)

    log_format = logging.Formatter(
        "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s"
    )

    stdout_log_handler = logging.StreamHandler()
    stdout_log_handler.setFormatter(log_format)
    stdout_log_handler.setLevel(logging.INFO)  # this can be higher
    logging.getLogger().setLevel(logging.DEBUG)
    logging.getLogger().addHandler(stdout_log_handler)

    # The activity log is for everything that isn't debug information. Only
    # write single lines and no exception tracebacks here as it is harder to
    # parse.
    try:
        log_file_handler = RotatingFileHandler(filename=LOG_FILE,
                                               maxBytes=1000000)
    except OSError as err:
        logger.critical("MOX_ROLLE_LOG_FILE: %s: %r", err.strerror,
                        err.filename)
        sys.exit(3)

    log_file_handler.setFormatter(log_format)
    log_file_handler.setLevel(logging.DEBUG)
    logging.getLogger().addHandler(log_file_handler)


class OpgavefordelerExporter:

    def __init__(self):
        cfg_file = pathlib.Path.cwd() / "settings" / "settings.json"
        if not cfg_file.is_file():
            raise Exception("No setting file")
        self.settings = json.loads(cfg_file.read_text())

        self.mora_base = self.settings.get("mora.base")
        self.mora_session = self._get_mora_session(token=os.environ.get("SAML_TOKEN"))

        self.opgavefordeler_base = self.settings.get("exporters.opgavefordeler.base")
        self.opgavefordeler_session = self._get_opgavefordeler_session(
            username=self.settings.get("exporters.opgavefordeler.username"),
            password=self.settings.get("exporters.opgavefordeler.password")
        )

        self.root_uuid = self.settings.get('exporters.opgavefordeler.root_uuid')

    def _get_mora_session(self, token) -> requests.Session:
        s = requests.Session()
        s.headers.update({"SESSION": token})

        retry = Retry(connect=20, backoff_factor=1)
        adapter = HTTPAdapter(max_retries=retry)
        s.mount('http://', adapter)
        s.mount('https://', adapter)
        return s

    def _get_opgavefordeler_session(self, username, password) -> requests.Session:
        s = requests.Session()
        s.auth = HTTPBasicAuth(username, password)
        return s

    def _get_mo_org_uuid(self) -> str:
        """
        Get the UUID of the organisation configured in OS2mo
        :return:
        """
        logger.info("Fetching Organisation UUID from OS2mo")
        r = requests.get("{}/service/o/".format(self.mora_base))
        r.raise_for_status()
        return r.json()[0]["uuid"]

    def _get_org_unit_children(self, org_unit_uuid: str) -> list:
        """
        Get the UUID of the organisation configured in OS2mo
        :return:
        """
        r = requests.get("{}/service/ou/{}/children".format(self.mora_base, org_unit_uuid))
        r.raise_for_status()
        return r.json()

    def _get_org_unit_manager(self, org_unit_uuid: str) -> dict:
        """
        Get the UUID of the organisation configured in OS2mo
        :return:
        """
        r = requests.get("{}/service/ou/{}/details/manager".format(self.mora_base, org_unit_uuid))
        r.raise_for_status()
        manager = r.json()

        if not manager:
            return {}

        if len(manager) > 1:
            logger.warning('More than one manager found for UUID {}'.format(org_unit_uuid))

        return manager[0]

    @lru_cache()
    def _get_employee(self, employee_uuid: str) -> dict:
        """
        Get the UUID of the organisation configured in OS2mo
        :return:
        """
        r = requests.get("{}/service/e/{}".format(self.mora_base, employee_uuid))
        r.raise_for_status()
        return r.json()

    @lru_cache()
    def _get_employee_detail(self, employee_uuid: str, detail: str) -> dict:
        """
        Get the details of a specific employee
        :return:
        """
        r = requests.get("{}/service/e/{}/details/{}".format(self.mora_base, employee_uuid, detail))
        r.raise_for_status()
        return r.json()

    @lru_cache()
    def _get_org_unit_detail(self, org_unit_uuid: str, detail: str) -> dict:
        """
        Get the details of a specific org unit
        :return:
        """
        r = requests.get("{}/service/ou/{}/details/{}".format(self.mora_base, org_unit_uuid, detail))
        r.raise_for_status()
        return r.json()

    @staticmethod
    def convert_unit(unit):
        converted = {
            "businessKey": unit['uuid'],
            "name": unit['name'],
            "esdhId": unit['uuid'],
            "esdhLabel": unit['name'],
        }
        return converted

    def convert_employee(self, engagement):
        person_uuid = engagement['person']['uuid']
        employee = self._get_employee(person_uuid)
        addresses = self._get_employee_detail(person_uuid, 'address')

        phone = {}
        phones = list(
            filter(lambda x: x['address_type']['scope'] == 'PHONE', addresses))
        if phones:
            phone = phones[0]

        email = {}
        emails = list(
            filter(lambda x: x['address_type']['scope'] == 'EMAIL', addresses))
        if emails:
            email = emails[0]

        # Eliminate extra whitespace
        name = str.join(' ', employee['name'].split())

        return {
            "businessKey": engagement['uuid'],
            "name": name,
            "email": email.get('value', ""),
            "esdhId": engagement['uuid'],
            "esdhLabel": name,
            "phone": phone.get('value', ""),
            "initials": employee['user_key'],
            "jobTitle": engagement['job_function']['name']
        }

    def handle_unit(self, unit):
        logger.debug('Processing unit {}'.format(unit['uuid']))

        # Convert unit details
        converted_unit = self.convert_unit(unit)

        converted_unit['children'] = self.handle_children(unit)
        converted_unit['manager'] = self.handle_manager(unit)
        employees = self.handle_employees(unit)
        if employees:
            converted_unit['employees'] = employees

        return converted_unit

    def handle_children(self, unit):
        children = []
        if unit['child_count'] > 0:
            child_units = self._get_org_unit_children(unit['uuid'])
            for child in child_units:
                children.append(self.handle_unit(child))
        return children

    def handle_manager(self, unit):
        logger.debug('Handling manager for {}'.format(unit['uuid']))
        manager = self._get_org_unit_manager(unit['uuid'])

        if not manager:
            return {}

        person = manager['person']
        if not person:
            logger.warning('Vacant manager for org unit {}'.format(unit['uuid']))
            return {}
        engagements = self._get_employee_detail(person['uuid'], 'engagement')
        if len(engagements) > 1:
            logger.warning('More than one engagement active for employee {}'.format(person['uuid']))
        engagement = engagements[0]

        return self.convert_employee(engagement)

    def handle_employees(self, unit):
        logger.debug('Handling employees for {}'.format(unit['uuid']))
        engagements = self._get_org_unit_detail(unit['uuid'], 'engagement')

        converted = [self.convert_employee(engagement) for engagement in engagements]
        return converted

    def submit_to_opgavefordeler(self, payload):
        logger.info("Submitting payload to Opgavefordeler")
        url = self.opgavefordeler_base
        r = self.opgavefordeler_session.post(url, json=payload)
        logger.info("Opgavefordeler response: {}".format(r.text))

        # This status code can occur, while still posting the payload
        # successfully - It's a known bug
        if r.status_code == 504:
            logger.warning(
                "Opgavefordeler returned 504 - payload was "
                "probably submitted successfully")
        else:
            r.raise_for_status()

    def run(self):
        init_log()

        org_uuid = self._get_mo_org_uuid()
        children_url = "{}/service/o/{}/children".format(self.mora_base, org_uuid)
        children = self.mora_session.get(children_url).json()

        root_unit = ""
        for child in children:
            if child['uuid'] == self.root_uuid:
                root_unit = child

        # Process root unit
        payload = self.handle_unit(root_unit)

        self.submit_to_opgavefordeler(payload)


if __name__ == "__main__":
    exporter = OpgavefordelerExporter()
    exporter.run()
