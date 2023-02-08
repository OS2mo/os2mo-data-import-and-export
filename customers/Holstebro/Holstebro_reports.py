import os
from pathlib import Path
from ra_utils.job_settings import JobSettings

from os2mo_helpers.mora_helpers import MoraHelper
from reports.query_actualstate import (
    list_employees,
    list_MED_members,
    run_report,
    list_org_units,
)
from exporters import common_queries as cq
from exporters.ballerup import export_udvalg


MORA_BASE = os.environ.get("MORA_BASE", "http://localhost:5000")


if __name__ == "__main__":
    # Læs fra settings
    settings = JobSettings()
    settings.start_logging_based_on_settings()

    mh = MoraHelper(hostname=MORA_BASE, export_ansi=False, use_cache=True)
    org = mh.read_organisation()
    roots = mh.read_top_units(org)

    for root in roots:
        if root["name"] == "Holstebro Kommune":
            holstebro = root["uuid"]

        # Taken from Holstebro frontend.
        if root["name"] == "MED-organisation":
            test_med = root["uuid"]

    nodes = mh.read_ou_tree(holstebro)

    print("BEFORE FIRST REPORT!")
    run_report(
        list_MED_members,
        "MED-Organisation",
        {"løn": "Holstebro Kommune", "MED": "MED-organisationen"},
        "/opt/docker/os2mo/queries/MED_medlemmer.xlsx",
    )
    print("DONE WITH FIRST REPORT!")
    print("BEFORE SECOND REPORT NOW!")
    run_report(
        list_employees,
        "Ansatte",
        "Holstebro Kommune",
        "/opt/docker/os2mo/queries/Ansatte.xlsx",
    )
    print("DONE WITH SECOND REPORT NOW!")
    print("BEFORE THIRD REPORT NOW!")
    run_report(
        list_org_units,
        "Organsiationsenheder",
        "Svendborg Kommune",
        "/opt/docker/os2mo/queries/Organisationsenheder.xlsx",
    )
    print("DONE WITH THIRD REPORT NOW!")

    # path = Path("/opt/docker/os2mo/queries/")
    #
    # filename = "/opt/docker/os2mo/queries/Test_Alle_lederfunktioner_os2mo.csv"
    # cq.export_managers(mh, nodes, filename, empty_manager_fields=True)
    #
    # filename = "/opt/docker/os2mo/queries/Test_AlleBK-stilling-email_os2mo.csv"
    # cq.export_all_employees(mh, nodes, filename)
    #
    # filename = (
    #     "/opt/docker/os2mo/queries/Test_Holstebro_org_incl-medarbejdere_os2mo.csv"
    # )
    # cq.export_orgs(mh, nodes, filename)
    #
    # filename = "/opt/docker/os2mo/queries/Test_Adm-org-incl-start-og-stopdata-og-enhedstyper-os2mo.csv"
    # cq.export_adm_org(mh, nodes, filename)
    #
    # # TODO Test this one out AFTER fixing MoraHelpers
    # print("IT REACHES THIS POINT 1")
    # filename = "/opt/docker/os2mo/queries/Test_teams_teamteams-tilknyttede-os2mo.csv"
    # cq.export_all_teams(mh, nodes, filename)
    # print("IT REACHES THIS POINT 2")
    #
    # # Check if this one even works - does Holstebro have AMR?
    # # TODO Test this one out still - export_udvalg
    # nodes = mh.read_ou_tree(test_med)
    # filename = "/opt/docker/os2mo/queries/Test_AMR-udvalgsmedlemer_i_hieraki.csv"
    # fieldnames = ["Hoved-MED", "Center-MED", "Lokal-MED", "AMR-Gruppe"]
    # org_types = ["AMR"]
    # export_udvalg(mh, nodes, filename, fieldnames, org_types)
    #
    # # TODO Test this one out still - export_udvalg
    # filename = "/opt/docker/os2mo/queries/Test_MED-udvalgsmedlemer_i_hieraki.csv"
    # fieldnames = ["Hoved-MED", "Center-MED", "Lokal-MED", "AMR-Gruppe"]
    # org_types = ["Hoved-MED"]  # What kind of org_types is included? We have
    # # "F-MED Børn og Unge", "F-MED Kultur, Erhverv og Arbejdsmarks" etc.
    # export_udvalg(mh, nodes, filename, fieldnames, org_types)
