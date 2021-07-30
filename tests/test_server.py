""" Test trapi-throttle server """
import asyncio
import copy
import datetime

import reasoner_pydantic
from starlette.responses import Response
import pytest
import httpx

from trapi_throttle.server import APP, startup_event, shutdown_event
from trapi_throttle.config import settings

from .utils import validate_message, with_kp_overlay, with_response_overlay

@pytest.fixture
async def client():
    async with httpx.AsyncClient(app=APP, base_url="http://test") as client:
        await startup_event()
        yield client
        await shutdown_event()


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
async def test_batch(client):
    """ Test that we correctly batch 3 queries into 1 """

    # Register kp
    kp_info = {
        "url": "http://kp1/query",
        "request_qty": 1,
        "request_duration": 1,
    }
    response = await client.post("/register/kp1", json=kp_info)
    assert response.status_code == 200

    # # Wait for batch processing thread to get ready
    # await asyncio.sleep(3)

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
    responses = await asyncio.wait_for(
        asyncio.gather(
            *(
                client.post(
                    "/kp1/query",
                    json={"message": {"query_graph": qg}}
                )
                for qg in qgs
            )
        ),
        timeout=20,
    )

    # Verify that everything was split correctly
    for index in range(len(responses)):
        curie = curies[index]
        msg = responses[index].json()["message"]
        print(msg)
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
    await asyncio.sleep(1)
    response = await client.get("/unregister/kp1")
    assert response.status_code == 200
    # await asyncio.sleep(1)


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
async def test_mixed_batching(client):
    """ Test that we handle a mixed of identical and differing queries """

    # Register kp
    kp_info = {
        "url": "http://kp1/query",
        "request_qty": 1,
        "request_duration": 1,
    }
    response = await client.post("/register/kp1", json=kp_info)
    assert response.status_code == 200

    # # Wait for batch processing thread to get ready
    # await asyncio.sleep(3)

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
    responses = await asyncio.wait_for(
        asyncio.gather(
            *(
                client.post(
                    "/kp1/query",
                    json={"message": {"query_graph": qg}}
                )
                for qg in qgs
            )
        ),
        timeout=20,
    )
    # print([response.json() for response in responses])

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

    # Remove registration
    await asyncio.sleep(1)
    response = await client.get("/unregister/kp1")
    assert response.status_code == 200
    # await asyncio.sleep(1)


@with_kp_overlay(
    "http://kp1/query",
    kp_data="""
        MONDO:0005148(( category biolink:Disease ))
        CHEBI:6801(( category biolink:ChemicalSubstance ))
        CHEBI:6801-- predicate biolink:treats -->MONDO:0005148
        """,
    request_qty=3,
    request_duration=datetime.timedelta(seconds=1)
)
@pytest.mark.asyncio
async def test_metakg(client):
    """Test /metakg."""

    # Register kp
    kp_info = {
        "url": "http://kp1/query",
        "request_qty": 1,
        "request_duration": 1,
    }
    response = await client.post("/register/kp1", json=kp_info)
    assert response.status_code == 200

    # get meta knowledge graph
    response = await client.get("/kp1/meta_knowledge_graph")
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_duplicate(client):
    """ Test that we correctly batch 3 queries into 1 """

    # Register kp
    kp_info = {
        "url": "http://kp1/query",
        "request_qty": 1,
        "request_duration": 1,
    }
    response = await client.post("/register/kp1", json=kp_info)
    assert response.status_code == 200

    # and again, differently
    kp_info = {
        "url": "http://kp1/query",
        "request_qty": 2,
        "request_duration": 1,
    }
    response = await client.post("/register/kp1", json=kp_info)
    assert response.status_code == 409


@with_kp_overlay(
    "http://kp1/query",
    kp_data="""
        MONDO:0005148(( category biolink:Disease ))
        CHEBI:6801(( category biolink:ChemicalSubstance ))
        CHEBI:6801-- predicate biolink:treats -->MONDO:0005148
        """,
    request_qty=100,
    request_duration=datetime.timedelta(seconds=1)
)
@pytest.mark.asyncio
async def test_no_rate_limit(client):
    """ Test that registration with request_qty = 0 enforces no rate limit """

    # Register kp
    kp_info = {
        "url": "http://kp1/query",
        "request_qty": 0,
        "request_duration": 1,
    }
    response = await client.post("/register/kp1", json=kp_info)
    assert response.status_code == 200
    
    # Wait for batch processing thread to get ready
    await asyncio.sleep(1)

    qg_template = {
        "nodes": {
            "n0": {"ids": ["CHEBI:6801"]},
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

    qgs = []

    # Make sure the requests can't be merged
    for i in range(10):
        qg = copy.deepcopy(qg_template)
        qg["nodes"]["n0"]["extra"] = i
        qgs.append(qg)

    start_time = datetime.datetime.utcnow()
    await asyncio.gather(
        *(
            client.post(
                "/query/kp1",
                json={"message": {"query_graph": qg}}
            )
            for qg in qgs
        )
    )
    end_time = datetime.datetime.utcnow()

    assert (start_time - end_time).total_seconds() < 0.1


@pytest.mark.asyncio
@with_response_overlay(
    "http://kp1/query",
    response=Response("Internal server error", 500),
    request_qty=5,
    request_duration=datetime.timedelta(seconds=1)
)
async def test_kp_500(client):
    """ Test that we return errors if the KP fails to respond """

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

    empty_qg = {
        "nodes": {},
        "edges": {},
    }

    responses = await asyncio.gather(
        *(
            client.post(
                "/kp1/query",
                json={"message": {"query_graph": empty_qg}}
            )
            for _ in range(5)
        )
    )

    # Check that all of the requests are served and contain the error message
    for r in responses:
        # Check that we keep the status code
        assert r.status_code == 500
        # Check that this is a LogEntry
        reasoner_pydantic.LogEntry.parse_raw(r.text)
        # Check that we kept the message from the KP
        r = r.json()
        assert r["response"]["data"] == "Internal server error"


@pytest.mark.asyncio
async def test_kp_unreachable(client):
    """ Test that we return errors if the KP is not reachable """

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

    empty_qg = {
        "nodes": {},
        "edges": {},
    }

    responses = await asyncio.gather(
        *(
            client.post(
                "/kp1/query",
                json={"message": {"query_graph": empty_qg}}
            )
            for _ in range(5)
        )
    )

    # Check that all of the requests are served and contain the error message
    for r in responses:
        # Check that we keep the status code
        assert r.status_code == 502
        # Check that this is a LogEntry
        reasoner_pydantic.LogEntry.parse_raw(r.text)
        r = r.json()
        # Check that we add a message
        assert r["message"] == "Request Error contacting KP"
