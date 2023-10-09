from functools import lru_cache
from pathlib import Path
from ra_utils.job_settings import JobSettings
from raclients.graph.client import GraphQLClient


class EngagementSettings(JobSettings):
    # common settings for clients:
    yesterdays_json_report_path: Path = Path(
        "/opt/dipex/os2mo/os2mo-data-import-and-export/"
        "reports/os2mo_new_and_ended_engagement_reports/employee_uuids_yesterday.json"
    )
    todays_json_report_path: Path = Path(
        "/opt/dipex/os2mo/os2mo-data-import-and-export/employee_uuids_today.json"
    )

    report_new_persons_file_path: str = "new_persons.csv"
    report_ended_engagements_file_path: str = "ended_engagements.csv"


@lru_cache()
def get_engagement_settings(*args, **kwargs) -> EngagementSettings:
    return EngagementSettings(*args, **kwargs)


def setup_gql_client(settings: EngagementSettings) -> GraphQLClient:

    return GraphQLClient(
        url=f"{settings.mora_base}/graphql/v3",
        client_id=settings.client_id,
        client_secret=settings.client_secret,
        auth_realm=settings.auth_realm,
        auth_server=settings.auth_server,
        sync=True,
        httpx_client_kwargs={"timeout": None},
    )
