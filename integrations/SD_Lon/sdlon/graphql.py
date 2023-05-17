from raclients.graph.client import GraphQLClient


def get_mo_client(
    auth_server: str,
    client_id: str,
    client_secret: str,
    mo_base_url: str,
    timeout: int = 600,
) -> GraphQLClient:
    """
    Get the GraphQL client for calling MO.

    Args:
        auth_server: the Keycloak server
        client_id: Keycloak client ID
        client_secret: Keycloak client secret
        mo_base_url: MOs base URL
        timeout: timeout for the client

    Returns:
        A GraphQL client
    """

    return GraphQLClient(
        url=f"{mo_base_url}/graphql/v3",
        client_id=client_id,
        client_secret=client_secret,
        auth_server=auth_server,
        auth_realm="mo",
        execute_timeout=timeout,
        httpx_client_kwargs={"timeout": timeout},
        sync=True,
    )
