"""Test trapi-throttle library."""
import asyncio
import copy
import datetime
from trapi_throttle.trapi import BatchingError

from reasoner_pydantic.message import Query
import pytest

from .utils import validate_message, with_kp_overlay, with_response_overlay
from trapi_throttle.throttle import KPInformation, ThrottledServer


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
async def test_batch():
    """ Test that we correctly batch 3 queries into 1 """

    # Register kp
    kp_info = {
        "url": "http://kp1/query",
        "request_qty": 1,
        "request_duration": 1,
    }

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
    async with ThrottledServer("kp1", **kp_info) as server:
        msgs = await asyncio.wait_for(
            asyncio.gather(
                *(
                    server.query(
                        {"message": {"query_graph": qg}}
                    )
                    for qg in qgs
                )
            ),
            timeout=20,
        )

    # Verify that everything was split correctly
    for index in range(len(msgs)):
        curie = curies[index]
        msg = msgs[index]["message"]
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
    request_qty=1,
    request_duration=datetime.timedelta(seconds=1)
)
async def test_429():
    """Test that we catch 429s correctly."""
    kp_info = {
        "url": "http://kp1/query",
        "request_qty": 1,
        "request_duration": 0.75,
    }

    qgs = [
        {
            "nodes": {
                "n0": {"ids": ["CHEBI:6801"]},
                "n1": {"categories": ["biolink:Disease"]},
            },
            "edges": {
                "n0n1": {
                    "subject": "n0",
                    "object": "n1",
                    "predicates": ["biolink:related_to"],
                }
            },
        },
        {
            "nodes": {
                "n0": {"ids": ["CHEBI:6802"]},
                "n1": {"categories": ["biolink:Gene"]},
            },
            "edges": {
                "n0n1": {
                    "subject": "n0",
                    "object": "n1",
                    "predicates": ["biolink:related_to"],
                }
            },
        },
    ]

    async with ThrottledServer("kp1", **kp_info) as server:
        # Submit queries
        results = await asyncio.wait_for(
            asyncio.gather(
                *(
                    server.query(
                        {"message": {"query_graph": qg}}
                    )
                    for qg in qgs
                )
            ),
            timeout=20,
        )


@pytest.mark.asyncio
@with_response_overlay(
    "http://kp1/query",
    response={"message": {"knowledge_graph": {"nodes": {}, "edges": {}}, "query_graph": None}},
    request_qty=5,
    request_duration=datetime.timedelta(seconds=1)
)
async def test_no_qg():
    """Test that we handle KP responses with missing qgraph."""
    kp_info = {
        "url": "http://kp1/query",
        "request_qty": 1,
        "request_duration": 0.75,
    }

    qgs = [
        {
            "nodes": {
                "n0": {"ids": ["CHEBI:6801"]},
                "n1": {"categories": ["biolink:Disease"]},
            },
            "edges": {
                "n0n1": {
                    "subject": "n0",
                    "object": "n1",
                    "predicates": ["biolink:related_to"],
                }
            },
        },
    ]

    with pytest.raises(BatchingError, match=r"qgraph not returned"):
        async with ThrottledServer("kp1", **kp_info) as server:
            # Submit queries
            results = await asyncio.wait_for(
                asyncio.gather(
                    *(
                        server.query(
                            {"message": {"query_graph": qg}}
                        )
                        for qg in qgs
                    )
                ),
                timeout=20,
            )


@pytest.mark.asyncio
@with_response_overlay(
    "http://kp1/query",
    response={"message": {"knowledge_graph": None, "query_graph": {"nodes": {}, "edges": {}}}},
    request_qty=5,
    request_duration=datetime.timedelta(seconds=1)
)
async def test_no_kg():
    """Test that we handle KP responses with missing kgraph."""
    kp_info = {
        "url": "http://kp1/query",
        "request_qty": 1,
        "request_duration": 0.75,
    }

    qgs = [
        {
            "nodes": {
                "n0": {"ids": ["CHEBI:6801"]},
                "n1": {"categories": ["biolink:Disease"]},
            },
            "edges": {
                "n0n1": {
                    "subject": "n0",
                    "object": "n1",
                    "predicates": ["biolink:related_to"],
                }
            },
        },
    ]

    with pytest.raises(BatchingError, match=r"kgraph not returned"):
        async with ThrottledServer("kp1", **kp_info) as server:
            # Submit queries
            results = await asyncio.wait_for(
                asyncio.gather(
                    *(
                        server.query(
                            {"message": {"query_graph": qg}}
                        )
                        for qg in qgs
                    )
                ),
                timeout=20,
            )


QG = {
    "nodes": {
        "n0": {"ids": ["CHEBI:6801"]},
        "n1": {"categories": ["biolink:Disease"]},
    },
    "edges": {
        "n0n1": {
            "subject": "n0",
            "object": "n1",
            "predicates": ["biolink:related_to"],
        }
    },
}


@pytest.mark.asyncio
@with_response_overlay(
    "http://kp1/query",
    response={"message": {
        "knowledge_graph": {"nodes": {}, "edges": {}},
        "query_graph": QG,
        "results": [],
    }},
    request_qty=5,
    request_duration=datetime.timedelta(seconds=1),
    delay=2.0,
)
async def test_slow():
    """Test that we handle KP responses with missing results."""
    kp_info = {
        "url": "http://kp1/query",
        "request_qty": 1,
        "request_duration": 0.75,
    }

    with pytest.raises(asyncio.exceptions.TimeoutError):
        async with ThrottledServer("kp1", **kp_info) as server:
            # Submit queries
            result = await server.query(
                {"message": {"query_graph": QG}},
                timeout=1.0,
            )


@pytest.mark.asyncio
@with_response_overlay(
    "http://kp1/query",
    response={"message": {
        "knowledge_graph": {"nodes": {}, "edges": {}},
        "query_graph": QG,
        "results": [],
    }},
    request_qty=5,
    request_duration=datetime.timedelta(seconds=1),
    delay=10.0,
)
async def test_batched_timeout():
    """Test KP/batched-level timeout."""
    kp_info = {
        "url": "http://kp1/query",
        "request_qty": 1,
        "request_duration": 0.75,
    }

    with pytest.raises(asyncio.exceptions.TimeoutError):
        async with ThrottledServer(
            "kp1",
            **kp_info,
            timeout=1.0,
        ) as server:
            await server.query(
                {"message": {"query_graph": QG}},
                timeout=None,
            )


@pytest.mark.asyncio
@with_response_overlay(
    "http://kp1/query",
    response={"message": {
        "knowledge_graph": {"nodes": {}, "edges": {}},
        "query_graph": QG,
        "results": None,
    }},
    request_qty=5,
    request_duration=datetime.timedelta(seconds=1)
)
async def test_no_results():
    """Test that we handle KP responses with missing results."""
    kp_info = {
        "url": "http://kp1/query",
        "request_qty": 1,
        "request_duration": 0.75,
    }

    async with ThrottledServer("kp1", **kp_info) as server:
        # Submit queries
        result = await server.query(
            {"message": {"query_graph": QG}}
        )
    assert result["message"]["results"] == []


@pytest.mark.asyncio
@with_kp_overlay(
    "http://kp1/query",
    kp_data="""
        CHEBI:6801(( category biolink:ChemicalSubstance ))
        CHEBI:6802(( category biolink:ChemicalSubstance ))
        MONDO:0005148(( category biolink:Disease ))
        MONDO:0005149(( category biolink:Disease ))
        CHEBI:6801-- predicate biolink:treats -->MONDO:0005148
        CHEBI:6802-- predicate biolink:treats -->MONDO:0005148
        """,
    request_qty=3,
    request_duration=datetime.timedelta(seconds=1)
)
async def test_double_pinned():
    """Test that we correctly handle qedges with both ends pinned."""

    # Register kp
    kp_info = {
        "url": "http://kp1/query",
        "request_qty": 1,
        "request_duration": 1,
    }

    qgs = [
        {
            "nodes": {
                "n0": {"ids": ["CHEBI:6801"]},
                "n1": {"ids": ["MONDO:0005148"]},
            },
            "edges": {
                "n0n1": {
                    "subject": "n0",
                    "object": "n1",
                    "predicates": ["biolink:treats"],
                }
            },
        },
        {
            "nodes": {
                "n0": {"ids": ["CHEBI:6802"]},
                "n1": {"ids": ["MONDO:0005149"]},
            },
            "edges": {
                "n0n1": {
                    "subject": "n0",
                    "object": "n1",
                    "predicates": ["biolink:treats"],
                }
            },
        }
    ]

    # Submit queries
    async with ThrottledServer("kp1", **kp_info) as server:
        msgs = await asyncio.wait_for(
            asyncio.gather(
                *(
                    server.query(
                        {"message": {"query_graph": qg}}
                    )
                    for qg in qgs
                )
            ),
            timeout=20,
        )
        
    print(msgs)
