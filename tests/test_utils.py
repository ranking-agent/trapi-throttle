import asyncio
import datetime

import httpx
from starlette.responses import Response
import pytest

from .utils import with_response_overlay

@pytest.mark.asyncio
@with_response_overlay(
    "http://kp/query",
    response = Response(status_code=200, content="OK"),
    request_qty = 3,
    request_duration = datetime.timedelta(seconds = 1)
)
async def test_rate_limiting():
    """ Test that our utility processes rate limits correctly """
    async with httpx.AsyncClient() as client:
        response = await client.get("http://kp/query/1")
        assert response.status_code == 200
        response = await client.get("http://kp/query/2")
        assert response.status_code == 200
        response = await client.get("http://kp/query/3")
        assert response.status_code == 429

        await asyncio.sleep(1)

        response = await client.get("http://kp/query")
        assert response.status_code == 200
