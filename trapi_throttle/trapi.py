from trapi_throttle.utils import all_equal
from reasoner_pydantic import Message, QueryGraph

class UnableToMerge(BaseException):
    """ Unable to merge given query graphs """

def merge_qgraphs_by_id(qgraphs: list[QueryGraph]) -> QueryGraph:
    """
    Merge query graphs using only the ids property on nodes

    If any other properties differ this will raise UnableToMerge
    """

    # Initialize map of IDs contained in each node by key
    node_id_mapping = {
        node_id:[] for node_id, node in qgraphs[0]["nodes"].items()
        if "ids" in node
    }

    # Fill in map and remove IDs from nodes
    for node_id, id_list in node_id_mapping.items():
        for qgraph in qgraphs:
            try:
                id_list.extend(
                    qgraph["nodes"][node_id].pop("ids")
                )
            except KeyError:
                # ids property not present
                raise UnableToMerge(
                    "ID properties not present on all messages"
                )

    # With ids removed all of the dictionaries should be equal
    if not all_equal(qgraphs):
        raise UnableToMerge("Query graphs are not equal")

    # Re-add IDs to the merged query graph
    merged_qgraph = qgraphs[0]
    for node_id, id_list in node_id_mapping.items():
        merged_qgraph["nodes"][node_id]["ids"] = id_list

    return merged_qgraph


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
        nid:node for nid,node in message["knowledge_graph"]["nodes"].items()
        if nid in bound_knodes
    }
    message["knowledge_graph"]["edges"] = {
        eid:edge for eid,edge in message["knowledge_graph"]["edges"].items()
        if eid in bound_kedges
    }

def filter_by_kgraph_id(message, kgraph_node_id):
    """
    Filter a message to ensure that all results
    and edges are associated with the given kgraph node ID
    """

    # Only keep results where there is a node binding
    # that connects to our given kgraph_node_id
    message["results"] = [
        result for result in message["results"]
        if any(
            nb["id"] == kgraph_node_id
            for qg_id, nb_list in result["node_bindings"].items()
            for nb in nb_list
        )
    ]

    remove_unbound_from_kg(message)
