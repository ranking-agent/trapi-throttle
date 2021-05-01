""" Test trapi-throttle server """
import asyncio
import datetime
import tempfile

from starlette.responses import JSONResponse
import pytest
import httpx
from asgi_lifespan import LifespanManager

from trapi_throttle.config import settings
settings.redis_url = "redis://localhost:6379/0"

from trapi_throttle.server import APP
from .utils import with_response_overlay

@pytest.fixture
async def client():
    async with httpx.AsyncClient(app=APP, base_url="http://test") as client, \
               LifespanManager(APP):
        yield client

@pytest.fixture
async def local_redis():
    # Create a temp file with our config
    config_file = tempfile.NamedTemporaryFile()

    config_file.write(b"notify-keyspace-events KEA\n")
    config_file.flush()

    # Start up redis in subprocess
    redis_process = await asyncio.create_subprocess_shell(
        f"redis-server {config_file.name}",
        stdout = asyncio.subprocess.PIPE,
    )

    # Read output until we see ready message
    while True:
        line = await redis_process.stdout.readline()
        print(line)
        if "Ready to accept connections" in line.decode("utf-8"):
            break

    yield
    redis_process.terminate()
    config_file.close()

TEST_QUERY = {
        "message" : {
            "query_graph" : {"nodes" : {}, "edges" : {}},
        }
    }
TEST_RESPONSE = {
        "message" : {
            "query_graph" : {"nodes" : {}, "edges" : {}},
            "knowledge_graph" : {"nodes" : {}, "edges" : {}},
            "results" : [],
        }
    }

@pytest.mark.asyncio
@with_response_overlay(
    "http://kp1/query",
    response = JSONResponse(content = TEST_RESPONSE),
    request_qty = 3,
    request_duration = datetime.timedelta(seconds = 1)
)
async def test_simple_rate_limit(local_redis, client):

    # Register kp
    kp_info = {
        "url" : "http://kp1/query",
        "request_qty" : 3,
        "request_duration" : 1,
    }
    response = await client.post("/register/kp1", json=kp_info)
    assert response.status_code == 200

    # Wait for batch processing thread to get ready
    await asyncio.sleep(1)

    # Submit queries
    responses = await asyncio.gather(*[
        client.post("/query/kp1", json=TEST_QUERY)
        for _ in range(10)
    ])

    for r in responses:
        assert r.status_code == 200
        assert r.json()["message"]
