""" Test trapi-throttle server """
import pytest
import httpx

from trapi_throttle.server import APP

client = httpx.AsyncClient(app=APP, base_url="http://test")

TEST_QUERY = {
        "message" : {
            "query_graph" : {"nodes" : {}, "edges" : {}},
        }
    }

@pytest.mark.asyncio
async def test_query():
    response = await client.post("/query/hello", json=TEST_QUERY)
    assert response.json()["message"]
