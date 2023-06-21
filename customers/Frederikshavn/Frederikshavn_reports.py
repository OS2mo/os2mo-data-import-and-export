import logging
import sys

from ra_utils.load_settings import load_settings

from reports.query_actualstate import list_employees
from reports.query_actualstate import list_MED_members
from reports.query_actualstate import run_report

LOG_LEVEL = logging.DEBUG

logger = logging.getLogger("Frederikshavn_reports")

logging.basicConfig(
    format="%(levelname)s %(asctime)s %(name)s %(message)s",
    level=LOG_LEVEL,
    stream=sys.stdout,
)

if __name__ == "__main__":
    # Læs fra settings
    settings = load_settings()
    query_path = settings["mora.folder.query_export"]
    logger.debug("Running reports for Frederikshavn")
    # Lav rapport over MED_medlemmer
    run_report(
        list_MED_members,
        "MED",
        {"løn": "Frederikshavn Kommune", "MED": "MED-organisationen"},  # type: ignore
        query_path + "/MED_medlemmer.xlsx",
    )
    logger.debug("MED report done.")

    # Lav rapport over Ansatte i kommunen.
    run_report(
        list_employees,
        "Ansatte",
        "Frederikshavn Kommune",
        query_path + "/Ansatte.xlsx",
    )
    logger.debug("Employee report done.")

    logger.debug("All reports for Frederikshavn done")
