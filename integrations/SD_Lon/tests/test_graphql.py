from raclients.auth import keycloak_token_endpoint

from sdlon.graphql import get_mo_client


def test_get_mo_client():
    # Act
    client = get_mo_client(
        "http://keycloak-service:8080/auth",
        "client_id",
        "client_secret",
        "http://mo-service:5000",
        10,
        300,
    )

    # Assert
    assert client.transport.url == "http://mo-service:5000/graphql/v10"
    assert client.transport.client_args["token_endpoint"] == keycloak_token_endpoint(
        "http://keycloak-service:8080/auth", "mo"
    )
    assert client.transport.client_args["client_id"] == "client_id"
    assert client.transport.client_args["client_secret"] == "client_secret"
    assert client.transport.client_args["timeout"] == 300
