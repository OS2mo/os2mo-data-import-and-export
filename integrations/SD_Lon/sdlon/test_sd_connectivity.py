import logging
from uuid import UUID

import click
from pydantic import ValidationError

from .config import get_changed_at_settings
from .sd_common import sd_lookup


LOG_LEVEL = logging.DEBUG
LOG_FILE = "test_sd_connectivity.log"


def setup_logging():
    detail_logging = "sdCommon"
    for name in logging.root.manager.loggerDict:
        if name in detail_logging:
            logging.getLogger(name).setLevel(LOG_LEVEL)
        else:
            logging.getLogger(name).setLevel(logging.ERROR)

    logging.basicConfig(
        format="%(levelname)s %(asctime)s %(name)s %(message)s",
        level=LOG_LEVEL,
        filename=LOG_FILE,
    )


class TestSdConnectivity(object):
    def __init__(self):
        self.validation_error = None
        try:
            self.settings = get_changed_at_settings()
        except ValidationError as err:
            self.validation_error = err

    def _check_sd_settings(self):
        print("Check settings...")

        if self.validation_error is None:
            print("SD settings OK")
        else:
            print("The following SD settings errors were detected:")
            print(self.validation_error)

        print("Done checking SD settings\n\n")

    def _check_contact_to_sd(self):
        print("Tjekker at vi har kontakt til SD:")
        params = {
            "UUIDIndicator": "true",
            "InstitutionIdentifier": self.settings.sd_institution_identifier,
        }
        try:
            institution_info = sd_lookup(
                "GetInstitution20111201",
                settings=self.settings,
                params=params,
                use_cache=False,
            )
        except Exception as e:
            print("Fejl i kontakt til SD Løn: {}".format(e))
            exit(1)

        try:
            institution = institution_info["Region"]["Institution"]
            institution_uuid = institution["InstitutionUUIDIdentifier"]
            UUID(institution_uuid, version=4)
            print(" * Korrekt kontakt til SD Løn")
        except Exception as e:
            msg = " * Fik forbindelse, men modtog ikke-korrekt svar fra SD: {}, {}"
            print(msg.format(institution_uuid, e))
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
    setup_logging()
    check_connectivity()
