import pytest
from trapi_throttle.trapi import UnableToMerge, merge_qgraphs_by_id

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


def test_split_messages_simple():
    """ Test that we can split messages based on query graph IDs """
