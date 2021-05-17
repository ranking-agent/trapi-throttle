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

def split_messages_by_id(message: Message) -> list[Message]:
    """ Split a message """
    pass
