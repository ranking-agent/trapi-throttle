""" Test trapi-throttle server """
import asyncio
import copy
import datetime
import tempfile

from starlette.responses import JSONResponse
import pytest
import httpx
from asgi_lifespan import LifespanManager

from trapi_throttle.server import APP
from .utils import with_kp_overlay

@pytest.fixture
async def client():
    async with httpx.AsyncClient(app=APP, base_url="http://test") as client, \
               LifespanManager(APP):
        yield client


@pytest.mark.asyncio
@with_kp_overlay(
    "http://kp1/query",
    kp_data = \
        """
        CHEBI:6801(( category biolink:ChemicalSubstance ))
        MONDO:0005148(( category biolink:Disease ))
        CHEBI:6801-- predicate biolink:treats -->MONDO:0005148
        """,
    request_qty = 3,
    request_duration = datetime.timedelta(seconds = 1)
)
async def test_batch(client):

    # Register kp
    kp_info = {
        "url" : "http://kp1/query",
        "request_qty" : 1,
        "request_duration" : 1,
    }
    response = await client.post("/register/kp1", json=kp_info)
    assert response.status_code == 200

    # Wait for batch processing thread to get ready
    await asyncio.sleep(1)

    qg_template = {
        "nodes" : {
            "n0" : { "ids" : [] },
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

    # Build different query graphs
    curies = ["CHEBI:6801", "CHEBI:6802", "CHEBI:6803"]
    qgs = []
    for c in curies:
        qg = copy.deepcopy(qg_template)
        qg["nodes"]["n0"]["ids"] = [c]
        qgs.append(qg)

    # Submit queries
    responses = await asyncio.gather(
        *(
            client.post(
                "/query/kp1",
                json = {"message" : { "query_graph" : qg}}
            )
            for qg in qgs
        )
    )

    # Verify that everything was split correctly
    for index in range(len(responses)):
        msg = responses[index].json()["message"]
        assert "knowledge_graph" in msg
        # Check that the corresponding node is present
        assert curies[index] in msg["knowledge_graph"]["nodes"]
