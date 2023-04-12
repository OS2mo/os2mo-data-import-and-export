from functools import lru_cache
from pathlib import Path
from ra_utils.job_settings import JobSettings
from raclients.graph.client import GraphQLClient


class CommonQueryExportSettings(JobSettings):
    # common settings for clients:
    alle_leder_funktioner_file_path: Path = Path("/opt/docker/os2mo/queries/Alle_lederfunktioner_os2mo.csv")

    alle_bk_stilling_email_file_path: Path = Path("/opt/docker/os2mo/queries/AlleBK-stilling-email_os2mo.csv")

    ballerup_org_inc_medarbejdere_file_path: Path = Path(
        "/opt/docker/os2mo/queries/Ballerup_org_incl-medarbejdere_os2mo.csv")

    adm_org_incl_start_og_stopdata_og_enhedstyper_file_path: Path = Path(
        "/opt/docker/os2mo/queries/Adm-org-incl-start-og-stopdata-og-enhedstyper-os2mo.csv")

    teams_tilknyttede_file_path: Path = Path("/opt/docker/os2mo/queries/teams-tilknyttede-os2mo.csv")

    sd_loen_org_med_pnr_file_path: Path = Path("/opt/docker/os2mo/queries/SD-løn org med Pnr_os2mo.csv")

    amr_udvalgsmedlemmer_i_hieraki_file_path: Path = Path("/opt/docker/os2mo/queries/AMR-udvalgsmedlemer_i_hieraki.csv")

    med_udvalgsmedlemmer_i_hieraki_file_path: Path = Path("/opt/docker/os2mo/queries/MED-udvalgsmedlemer_i_hieraki.csv")


@lru_cache()
def get_common_query_export_settings(*args, **kwargs) -> CommonQueryExportSettings:
    return CommonQueryExportSettings(*args, **kwargs)


def setup_gql_client(settings: CommonQueryExportSettings) -> GraphQLClient:
    return GraphQLClient(
        url=f"{settings.mora_base}/graphql/v3",
        client_id=settings.client_id,
        client_secret=settings.client_secret,
        auth_realm=settings.auth_realm,
        auth_server=settings.auth_server,
        sync=True,
        httpx_client_kwargs={"timeout": None},
    )
