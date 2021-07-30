"""Test trapi-throttle library."""
import asyncio
import copy
import datetime

from reasoner_pydantic.message import Query
import pytest

from .utils import validate_message, with_kp_overlay
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
    kp_info = KPInformation(**{
        "url": "http://kp1/query",
        "request_qty": 1,
        "request_duration": 1,
    })

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
    async with ThrottledServer("kp1", kp_info) as server:
        msgs = await asyncio.wait_for(
            asyncio.gather(
                *(
                    server.query(
                        Query(**{"message": {"query_graph": qg}})
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
