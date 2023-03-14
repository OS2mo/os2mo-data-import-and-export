import time

from functools import lru_cache
from pathlib import Path
from ra_utils.job_settings import JobSettings
from raclients.graph.client import GraphQLClient


timestamp = time.strftime("%Y%m%d%H%M%S")


class EngagementSettings(JobSettings):
    # common settings for clients:
    read_yesterdays_json_report_path: Path = Path(
        "/opt/docker/os2mo/queries/read_new_entries_in_mo.json"
    )
    write_todays_json_report_path: Path = Path(
        f"/opt/docker/os2mo/queries/write_new_entries_in_mo.json"
    )

    write_todays_json_report_path_with_timestamp: Path = Path(
        f"/opt/docker/os2mo/queries/write_new_entries_in_mo{timestamp}.json"
    )
    copy_todays_json_report_path_with_timestamp: Path = Path(
        f"/opt/docker/os2mo/queries/copy_new_entries_in_mo{timestamp}.json"
    )

    report_engagements_new_file_path: Path = Path(
        "/opt/docker/os2mo/queries/report_engagements_new.csv"
    )
    report_engagements_ended_file_path: Path = Path(
        "/opt/docker/os2mo/queries/report_engagements_ended.csv"
    )


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
