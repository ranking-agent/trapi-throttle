""" Test trapi-throttle server """
import pytest
import httpx
from asgi_lifespan import LifespanManager

from trapi_throttle.config import settings
settings.redis_url = "redis://fakeredis:6379/0"

from trapi_throttle.server import APP

@pytest.fixture
async def client():
    async with httpx.AsyncClient(app=APP, base_url="http://test") as client, \
               LifespanManager(APP):
        yield client

TEST_QUERY = {
        "message" : {
            "query_graph" : {"nodes" : {}, "edges" : {}},
        }
    }

@pytest.mark.asyncio
async def test_query(client):
    response = await client.post("/query/hello", json=TEST_QUERY)
    assert response.json()["message"]
