import asyncio
import datetime

import httpx
from starlette.responses import Response
import pytest

from .utils import with_response_overlay, with_kp_overlay

@pytest.mark.asyncio
@with_response_overlay(
    "http://kp/query",
    response = Response(status_code=200, content="OK"),
    request_qty = 2,
    request_duration = datetime.timedelta(seconds = 1)
)
async def test_response_rate_limiting():
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

@pytest.mark.asyncio
@with_kp_overlay(
    "http://kp/query",
    kp_data = \
        """
        MONDO:0005148(( category biolink:Disease ))
        CHEBI:6801(( category biolink:ChemicalSubstance ))
        CHEBI:6801-- predicate biolink:treats -->MONDO:0005148
        """,
    request_qty = 2,
    request_duration = datetime.timedelta(seconds = 1)
)
async def test_kp_rate_limiting():
    """ Test that our kp utility processes rate limits correctly """

    qg = {
        "nodes" : {
            "n0" : { "ids" : ["CHEBI:6801"] },
            "n1" : { "categories" : ["biolink:Disease"] },
        },
        "edges" : {
            "n0n1" : {
                "subject" : "n0",
                "object" : "n1",
                "predicates" : ["biolink:treats"],
            }
        },
    }

    query = {"message" : {"query_graph" : qg}}

    async with httpx.AsyncClient() as client:
        response = await client.post("http://kp/query", json = query)
        assert response.status_code == 200
        response = await client.post("http://kp/query", json = query)
        assert response.status_code == 200
        response = await client.post("http://kp/query", json = query)
        assert response.status_code == 429

        await asyncio.sleep(1)

        response = await client.post("http://kp/query", json = query)
        assert response.status_code == 200
