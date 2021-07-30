import copy

from reasoner_pydantic import Message, QueryGraph


class BatchingError(Exception):
    """Error batching TRAPI requests."""


class UnableToMerge(BaseException):
    """ Unable to merge given query graphs """


def get_curies(qgraph: QueryGraph) -> dict[str, list[str]]:
    """
    Pull curies from query graph and
    return them as a mapping of node_id -> curie_list
    """
    return {
        node_id: curies
        for node_id, node in qgraph["nodes"].items()
        if (curies := node.get("ids", None)) is not None
    }


def remove_curies(qgraph: QueryGraph) -> dict[str, list[str]]:
    """
    Remove curies from query graph.
    """
    qgraph = copy.deepcopy(qgraph)
    for node in qgraph["nodes"].values():
        node.pop("ids", None)
    return qgraph


def remove_unbound_from_kg(message):
    """
    Remove all knowledge graph nodes and edges without a binding
    """

    bound_knodes = set()
    for result in message["results"]:
        for node_binding_list in result["node_bindings"].values():
            for nb in node_binding_list:
                bound_knodes.add(nb["id"])
    bound_kedges = set()
    for result in message["results"]:
        for edge_binding_list in result["edge_bindings"].values():
            for nb in edge_binding_list:
                bound_kedges.add(nb["id"])

    message["knowledge_graph"]["nodes"] = {
        nid: node for nid, node in message["knowledge_graph"]["nodes"].items()
        if nid in bound_knodes
    }
    message["knowledge_graph"]["edges"] = {
        eid: edge for eid, edge in message["knowledge_graph"]["edges"].items()
        if eid in bound_kedges
    }


def result_contains_node_bindings(
        result,
        bindings: dict[str, list[str]]
):
    """ Check that the result object has all bindings provided (qg_id->kg_id) """
    for qg_id, kg_ids in bindings.items():
        if not any(
            nb["id"] in kg_ids
            for nb in result["node_bindings"][qg_id]
        ):
            return False
    return True


def filter_by_curie_mapping(
        message: Message,
        curie_mapping: dict[str, list[str]],
        kp_id: str = "KP",
) -> Message:
    """
    Filter a message to ensure that all results
    contain the bindings specified in the curie_mapping
    """
    message = copy.deepcopy(message)

    # Update query graph IDs
    for qg_id, curie_list in curie_mapping.items():
        if message["query_graph"] is None:
            raise BatchingError(f"qgraph not returned from {kp_id}")
        try:
            message["query_graph"]["nodes"][qg_id]["ids"] = curie_list
        except KeyError:
            raise BatchingError(f"qgraph from {kp_id} appears to be modified or malformed")

    # Only keep results where there is a node binding
    # that connects to our given kgraph_node_id
    message["results"] = [
        result for result in message["results"]
        if result_contains_node_bindings(result, curie_mapping)
    ]

    # Remove extra knowledge graph nodes
    remove_unbound_from_kg(message)

    return message
