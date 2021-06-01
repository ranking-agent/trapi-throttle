""" Test trapi-throttle server """
import asyncio
import copy
import datetime
import tempfile

import aioredis
from starlette.responses import JSONResponse
import pytest
import httpx
from asgi_lifespan import LifespanManager

from trapi_throttle.server import APP
from trapi_throttle.config import settings

from .utils import validate_message, with_kp_overlay


@pytest.fixture
async def client():
    async with httpx.AsyncClient(app=APP, base_url="http://test") as client, \
            LifespanManager(APP):
        yield client


@pytest.fixture
async def clear_redis():
    r = await aioredis.create_redis(settings.redis_url)
    await r.flushdb()
    yield


@pytest.mark.asyncio
@with_kp_overlay(
    "http://kp1/query",
    kp_data="""
        MONDO:0005148(( category biolink:Disease ))
        CHEBI:6801(( category biolink:ChemicalSubstance ))
        CHEBI:6801-- predicate biolink:treats -->MONDO:0005148
        CHEBI:6802(( category biolink:ChemicalSubstance ))
        CHEBI:6802-- predicate biolink:treats -->MONDO:0005148
        CHEBI:6803(( category biolink:ChemicalSubstance ))
        CHEBI:6803-- predicate biolink:treats -->MONDO:0005148
        """,
    request_qty=3,
    request_duration=datetime.timedelta(seconds=1)
)
async def test_batch(client, clear_redis):
    """ Test that we correctly batch 3 queries into 1 """

    # Register kp
    kp_info = {
        "url": "http://kp1/query",
        "request_qty": 1,
        "request_duration": 1,
    }
    response = await client.post("/register/kp1", json=kp_info)
    assert response.status_code == 200

    # Wait for batch processing thread to get ready
    await asyncio.sleep(1)

    qg_template = {
        "nodes": {
            "n0": {"ids": []},
            "n1": {"categories": ["biolink:Disease"]},
        },
        "edges": {
            "n0n1": {
                "subject": "n0",
                "object": "n1",
                "predicates": ["biolink:treats"],
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
                json={"message": {"query_graph": qg}}
            )
            for qg in qgs
        )
    )

    # Verify that everything was split correctly
    for index in range(len(responses)):
        curie = curies[index]
        msg = responses[index].json()["message"]
        validate_message(
            {
                "knowledge_graph":
                    f"""
                    {curie} biolink:treats MONDO:0005148
                    """,
                "results": [
                    f"""
                    node_bindings:
                        n0 {curie}
                        n1 MONDO:0005148
                    edge_bindings:
                        n0n1 {curie}-MONDO:0005148
                    """
                ],
            },
            msg
        )

    # Remove registration
    response = await client.get("/unregister/kp1")
    assert response.status_code == 200
    # Wait for unregistration
    await asyncio.sleep(1)


@pytest.mark.asyncio
@with_kp_overlay(
    "http://kp1/query",
    kp_data="""
        MONDO:0005148(( category biolink:Disease ))
        CHEBI:6801(( category biolink:ChemicalSubstance ))
        CHEBI:6801-- predicate biolink:treats -->MONDO:0005148
        CHEBI:6802(( category biolink:ChemicalSubstance ))
        CHEBI:6802-- predicate biolink:treats -->MONDO:0005148
        CHEBI:6803(( category biolink:ChemicalSubstance ))
        CHEBI:6803-- predicate biolink:affects -->MONDO:0005148
        """,
    request_qty=3,
    request_duration=datetime.timedelta(seconds=1)
)
async def test_mixed_batching(client, clear_redis):
    """ Test that we handle a mixed of identical and differing queries """

    # Register kp
    kp_info = {
        "url": "http://kp1/query",
        "request_qty": 1,
        "request_duration": 1,
    }
    response = await client.post("/register/kp1", json=kp_info)
    assert response.status_code == 200

    # Wait for batch processing thread to get ready
    await asyncio.sleep(1)

    qg_template = {
        "nodes": {
            "n0": {"ids": []},
            "n1": {"categories": ["biolink:Disease"]},
        },
        "edges": {
            "n0n1": {
                "subject": "n0",
                "object": "n1",
                "predicates": [],
            }
        },
    }

    qgs = []

    # Q1
    qg = copy.deepcopy(qg_template)
    qg["nodes"]["n0"]["ids"].append("CHEBI:6801")
    qg["edges"]["n0n1"]["predicates"].append("biolink:treats")
    qgs.append(qg)

    # Q2 (merged with Q1)
    qg = copy.deepcopy(qg_template)
    qg["nodes"]["n0"]["ids"].append("CHEBI:6802")
    qg["edges"]["n0n1"]["predicates"].append("biolink:treats")
    qgs.append(qg)

    # Q3 (not merged)
    qg = copy.deepcopy(qg_template)
    qg["nodes"]["n0"]["ids"].append("CHEBI:6803")
    # Different predicate
    qg["edges"]["n0n1"]["predicates"].append("biolink:affects")
    qgs.append(qg)

    # Submit queries
    responses = await asyncio.gather(
        *(
            client.post(
                "/query/kp1",
                json={"message": {"query_graph": qg}}
            )
            for qg in qgs
        )
    )

    # Verify that everything was split correctly

    # Q1
    validate_message(
        {
            "knowledge_graph":
                f"""
                CHEBI:6801 biolink:treats MONDO:0005148
                """,
            "results": [
                f"""
                node_bindings:
                    n0 CHEBI:6801
                    n1 MONDO:0005148
                edge_bindings:
                    n0n1 CHEBI:6801-MONDO:0005148
                """
            ],
        },
        responses[0].json()["message"]
    )

    # Q2
    validate_message(
        {
            "knowledge_graph":
                f"""
                CHEBI:6802 biolink:treats MONDO:0005148
                """,
            "results": [
                f"""
                node_bindings:
                    n0 CHEBI:6802
                    n1 MONDO:0005148
                edge_bindings:
                    n0n1 CHEBI:6802-MONDO:0005148
                """
            ],
        },
        responses[1].json()["message"]
    )

    # Q3
    validate_message(
        {
            "knowledge_graph":
                f"""
                CHEBI:6803 biolink:affects MONDO:0005148
                """,
            "results": [
                f"""
                node_bindings:
                    n0 CHEBI:6803
                    n1 MONDO:0005148
                edge_bindings:
                    n0n1 CHEBI:6803-MONDO:0005148
                """
            ],
        },
        responses[2].json()["message"]
    )
