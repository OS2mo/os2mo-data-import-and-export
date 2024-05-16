from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient
from gql.client import AsyncClientSession

from ..main import create_app


@pytest.fixture
def test_client() -> Iterator[TestClient]:
    """Create ASGI test client with associated lifecycles."""
    app = create_app()
    with TestClient(app) as client:
        yield client


@pytest.fixture
async def graphql_client(test_client: TestClient) -> AsyncClientSession:
    """Authenticated GraphQL codegen client for OS2mo."""
    return test_client.app_state["context"]["user_context"]["codegen_client"]


@pytest.fixture
async def legacy_graphql_session(test_client: TestClient) -> AsyncClientSession:
    """Authenticated GraphQL codegen client for OS2mo."""
    return test_client.app_state["context"]["legacy_graphql_session"]
