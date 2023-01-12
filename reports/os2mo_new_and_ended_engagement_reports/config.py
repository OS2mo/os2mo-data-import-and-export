from functools import lru_cache
from pathlib import Path
from ra_utils.job_settings import JobSettings
from raclients.graph.client import GraphQLClient


class EngagementSettings(JobSettings):
    # common settings for clients:
    report_engagements_new_file_path: Path = Path("/opt/docker/os2mo/queries/report_engagements_new.csv")
    report_engagements_ended_file_path: Path = Path("/opt/docker/os2mo/queries/report_engagements_ended.csv")


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
