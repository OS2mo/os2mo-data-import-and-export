import logging
from uuid import UUID

import click
from pydantic import ValidationError

from .config import get_changed_at_settings
from .sd_common import sd_lookup


class TestSdConnectivity(object):
    def __init__(self):
        self.validation_error = None
        try:
            self.settings = get_changed_at_settings()
            self.settings.start_logging_based_on_settings()
        except ValidationError as err:
            self.validation_error = err

    def _check_sd_settings(self):
        logging.info("Check settings...")

        if self.validation_error is None:
            logging.info("SD settings OK")
        else:
            logging.warning(
                "The following SD settings errors were detected: %s",
                self.validation_error,
            )

        logging.info("Done checking SD settings")

    def _check_contact_to_sd(self):
        logging.info("Tjekker at vi har kontakt til SD")
        params = {
            "UUIDIndicator": "true",
            "InstitutionIdentifier": self.settings.sd_institution_identifier,
        }
        try:
            institution_info = sd_lookup(
                "GetInstitution20111201",
                settings=self.settings,
                params=params,
            )
        except Exception:
            logging.exception("Fejl i kontakt til SD Løn")
            exit(1)

        try:
            institution = institution_info["Region"]["Institution"]
            institution_uuid = institution["InstitutionUUIDIdentifier"]
            UUID(institution_uuid, version=4)
            logging.info("Korrekt kontakt til SD Løn")
        except Exception:
            logging.exception(
                "Fik forbindelse, men modtog ikke-korrekt svar fra SD: %s",
                institution_info,
            )
            exit(1)

    def sd_check(self):
        self._check_sd_settings()
        self._check_contact_to_sd()


@click.command()
def check_connectivity():
    """Check SD configuration and connectivity."""
    tsc = TestSdConnectivity()
    tsc.sd_check()


if __name__ == "__main__":
    check_connectivity()
