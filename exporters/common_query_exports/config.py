from functools import lru_cache
from pathlib import Path
from ra_utils.job_settings import JobSettings
from raclients.graph.client import GraphQLClient


class CommonQueryExportSettings(JobSettings):
    # common settings for clients:
    file_export_path: Path = Path("/opt/docker/os2mo/queries")


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
