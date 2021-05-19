import copy
from tests.utils import validate_message
import pytest
from trapi_throttle.trapi import UnableToMerge, filter_by_curie_mapping, merge_qgraphs_by_id

def test_merge_qgraphs_simple():
    """ Test that we can merge two query graphs """
    QGRAPH_A = {
        "nodes" : {
            "n0" : { "ids" : ["CHEBI:6801"] },
            "n1" : { "categories" : ["biolink:ChemicalSubstance"] }
        }
    }
    QGRAPH_B = {
        "nodes" : {
            "n0" : { "ids" : ["CHEBI:6802"] },
            "n1" : { "categories" : ["biolink:ChemicalSubstance"] }
        }
    }

    RESULT = {
        "nodes" : {
            "n0" : { "ids" : ["CHEBI:6801", "CHEBI:6802"] },
            "n1" : { "categories" : ["biolink:ChemicalSubstance"] }
        }
    }

    assert merge_qgraphs_by_id([QGRAPH_A, QGRAPH_B]) == RESULT

def test_merge_not_matching():
    """ Test that we throw an exception if we give different qgraphs """
    QGRAPH_A = {
        "nodes" : {
            "n0" : { "ids" : ["CHEBI:6801"] },
            "n1" : { "categories" : ["biolink:Drug"] }
        }
    }
    QGRAPH_B = {
        "nodes" : {
            "n0" : { "ids" : ["CHEBI:6802"] },
            "n1" : { "categories" : ["biolink:ChemicalSubstance"] }
        }
    }

    with pytest.raises(UnableToMerge):
        merge_qgraphs_by_id([QGRAPH_A, QGRAPH_B])


def test_split_message():
    """ Test that we can split a message using the kgraph node IDs """
    COMBINED_MSG = \
        {
            "query_graph" : {
                "nodes" : {
                    "n0" : { "ids" : ["CHEBI:6801", "CHEBI:6802"] },
                    "n1" : { "categories" : ["biolink:Drug"] },
                },
                "edges" : {
                    "n0n1" : {
                        "subject" : "n0",
                        "object" : "n1",
                        "predicates" : ["related_to"],
                    }
                }
            },
            "knowledge_graph" : {
                "nodes" : {
                    "CHEBI:6801" : {},
                    "CHEBI:6802" : {},
                    "MONDO:0005148" : {},
                    "MONDO:0005149" : {},
                },
                "edges" : {
                    "a" : {"subject" : "CHEBI:6801", "object" : "MONDO:0005148", "predicate" : "biolink:treats"},
                    "b" : {"subject" : "CHEBI:6802", "object" : "MONDO:0005149", "predicate" : "biolink:treats"},
                }
            },
            "results" : [
                {
                    "node_bindings" : {
                        "n0" : [{ "id" : "CHEBI:6801" }],
                        "n1" : [{ "id" : "MONDO:0005148"}],
                    },
                    "edge_bindings" : {
                        "n0n1" : [{ "id" : "a" }]
                    },
                },
                {
                    "node_bindings" : {
                        "n0" : [{ "id" : "CHEBI:6802" }],
                        "n1" : [{ "id" : "MONDO:0005149" }],
                    },
                    "edge_bindings" : {
                        "n0n1" : [{ "id" : "b" }],
                    },
                }
            ]
        }

    filtered_1 = copy.deepcopy(COMBINED_MSG)
    filter_by_kgraph_id(filtered_1, "CHEBI:6801")
    validate_message(
        {
            "knowledge_graph":
                """
                CHEBI:6801 biolink:treats MONDO:0005148
                """,
            "results": [
                """
                node_bindings:
                    n0 CHEBI:6801
                    n1 MONDO:0005148
                edge_bindings:
                    n0n1 CHEBI:6801-MONDO:0005148
                """
            ],
        },
        filtered_1
    )

    filtered_2 = copy.deepcopy(COMBINED_MSG)
    filter_by_kgraph_id(filtered_2, "CHEBI:6802")
    validate_message(
        {
            "knowledge_graph":
                """
                CHEBI:6802 biolink:treats MONDO:0005149
                """,
            "results": [
                """
                node_bindings:
                    n0 CHEBI:6802
                    n1 MONDO:0005149
                edge_bindings:
                    n0n1 CHEBI:6802-MONDO:0005149
                """
            ],
        },
        filtered_2
    )
