from collections import defaultdict
from contextlib import asynccontextmanager, AsyncExitStack
from urllib.parse import urlparse
from functools import partial, wraps
import datetime
import inspect

from fastapi import FastAPI, Response
from starlette.middleware.base import BaseHTTPMiddleware
from asgiar import ASGIAR

def url_to_host(url):
    # TODO modify ASGIAR to accept a URL instead of a host
    return urlparse(url).netloc

class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Middleware to enforce a rate limit

    Returns 429 if the rate limit is exceeded
    """
    def __init__(self, app,
                 request_qty: int,
                 request_duration: datetime.timedelta
                 ):
        super().__init__(app)
        self.request_qty = request_qty
        self.request_duration = request_duration
        self.rate_limit_tracker = {}

    async def dispatch(self, request, call_next):
        if "start_time" not in self.rate_limit_tracker:
            self.rate_limit_tracker = {
                "start_time" : datetime.datetime.now(),
                "qty" : 0
            }
        time_since_last_request = \
            datetime.datetime.now() - self.rate_limit_tracker["start_time"]

        if time_since_last_request > self.request_duration:
            # Reset rate limit
            self.rate_limit_tracker["start_time"] = datetime.datetime.now()
            self.rate_limit_tracker["qty"] = 0

        # Check that we are not above quantity
        self.rate_limit_tracker["qty"] += 1
        if self.rate_limit_tracker["qty"] > self.request_qty:
            return Response(status_code = 429)

        response = await call_next(request)
        return response


@asynccontextmanager
async def response_overlay(
        url,
        response: Response,
        request_qty: int,
        request_duration: datetime.timedelta
):
    """
    Create a router that returns the specified
    response for all routes. Returns 429 if the provided
    rate limit is exceeded.
    """
    async with AsyncExitStack() as stack:
        app = FastAPI()

        # pylint: disable=unused-variable disable=unused-argument
        @app.api_route('/{path:path}', methods=["GET", "POST", "PUT", "DELETE"])
        async def all_paths(path):
            return response

        app.add_middleware(
            RateLimitMiddleware,
            request_qty = request_qty,
            request_duration = request_duration,
        )

        await stack.enter_async_context(
            ASGIAR(app, host=url_to_host(url))
        )
        yield

def with_context(context, *args_, **kwargs_):
    """Turn context manager into decorator."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            async with context(*args_, **kwargs_):
                await func(*args, **kwargs)
        return wrapper
    return decorator

with_response_overlay = partial(with_context, response_overlay)


def validate_message(template, value):
    """
    Validate a message against the given template

    Raises ValueError if anything doesn't match up.

    Templates must be a dictionary with "knowledge_graph" and "results"
    keys. Knowledge_graph should be a string representation of the knowledge
    graph edges. Results should be a list of strings each representing a results object.
    """

    template["knowledge_graph"] = inspect.cleandoc(template["knowledge_graph"])

    nodes = set()
    # Validate edges
    for edge_string in template["knowledge_graph"].splitlines():
        sub, predicate, obj = edge_string.split(" ")
        nodes.add(sub)
        nodes.add(obj)
        # Check that this edge exists
        if not any(
                edge["subject"] == sub and
                edge["object"] == obj and
                predicate in edge["predicate"]
            for edge in value["knowledge_graph"]["edges"].values()
        ):
            raise ValueError(
                f"Knowledge graph edge {edge_string} not found in message")

    # Validate nodes
    for node in nodes:
        if node not in value["knowledge_graph"]["nodes"].keys():
            raise ValueError(
                f"Knowledge graph node {node} not found in message")

    # Check for extra nodes or edges
    if len(nodes) != len(value["knowledge_graph"]["nodes"]):
        raise ValueError(
            "Extra nodes found in message knowledge_graph")
    if (
        len(template["knowledge_graph"].splitlines()) !=
        len(value["knowledge_graph"]["edges"])
    ):
        raise ValueError(
            "Extra edges found in message knowledge_graph")

    # Validate results
    for index, template_result_string in enumerate(template["results"]):

        # Parse string representation
        template_result_string = inspect.cleandoc(template_result_string)
        template_result = {}
        current_key = None
        for line in template_result_string.splitlines():
            if not line.startswith(" "):
                # Key (remove trailing colon)
                current_key = line[:-1]
                template_result[current_key] = []
            else:
                # Value
                template_result[current_key].append(line.strip())

        try:
            value_result = value["results"][index]
        except IndexError as e:
            raise ValueError("Expected more results") from e

        # Validate node bindings
        for node_binding_string in template_result["node_bindings"]:
            qg_node_id, *kg_node_ids = node_binding_string.split(" ")
            if qg_node_id not in value_result["node_bindings"]:
                raise ValueError(
                    f"Could not find binding for node {qg_node_id}")

            for kg_node_id in kg_node_ids:
                if not any(
                    nb["id"] == kg_node_id
                    for nb in value_result["node_bindings"][qg_node_id]
                ):
                    raise ValueError(
                        f"Expected node binding {qg_node_id} to {kg_node_id}")
            if len(value_result["node_bindings"][qg_node_id]) != len(kg_node_ids):
                raise ValueError(f"Extra node bindings found for {qg_node_id}")

        # Validate edge bindings
        for edge_binding_string in template_result["edge_bindings"]:
            qg_edge_id, *kg_edge_strings = edge_binding_string.split(" ")

            # Find KG edge IDs from the kg edge strings
            kg_edge_ids = []
            for kg_edge_string in kg_edge_strings:
                sub, obj = kg_edge_string.split("-")
                kg_edge_id = next(
                    kg_edge_id
                    for kg_edge_id, kg_edge in value["knowledge_graph"]["edges"].items()
                    if kg_edge["subject"] == sub and kg_edge["object"] == obj
                )
                kg_edge_ids.append(kg_edge_id)

            if qg_edge_id not in value_result["edge_bindings"]:
                raise ValueError(
                    f"Could not find binding for edge {qg_edge_id}")

            for kg_edge_id in kg_edge_ids:
                if not any(
                    nb["id"] == kg_edge_id
                    for nb in value_result["edge_bindings"][qg_edge_id]
                ):
                    raise ValueError(
                        f"Expected edge binding {qg_edge_id} to {kg_edge_id}")
            if len(value_result["edge_bindings"][qg_edge_id]) != len(kg_edge_ids):
                raise ValueError(f"Extra edge bindings found for {qg_edge_id}")

    # Check for extra results
    if len(template["results"]) != len(value["results"]):
        raise ValueError("Extra results found")


def query_graph_from_string(s):
    """
    Parse a query graph from Mermaid flowchart syntax.
    Useful for writing examples in tests.

    Syntax information can be found here:
    https://mermaid-js.github.io/mermaid/#/flowchart

    You can use this site to preview a query graph:
    https://mermaid-js.github.io/mermaid-live-editor/
    """

    # This usually comes from triple quoted strings
    # so we use inspect.cleandoc to remove leading indentation
    s = inspect.cleandoc(s)

    node_re = r"(?P<id>.*)\(\( (?P<key>.*) (?P<val>.*) \)\)"
    edge_re = r"(?P<src>.*)-- (?P<predicate>.*) -->(?P<target>.*)"
    qg = {"nodes": {}, "edges": {}}
    for line in s.splitlines():
        match_node = re.search(node_re, line)
        match_edge = re.search(edge_re, line)
        if match_node:
            node_id = match_node.group('id')
            if node_id not in qg['nodes']:
                qg['nodes'][node_id] = {}
            qg['nodes'][node_id][match_node.group('key')] = \
                match_node.group('val')
        elif match_edge:
            edge_id = match_edge.group('src') + match_edge.group('target')
            qg['edges'][edge_id] = {
                "subject": match_edge.group('src'),
                "object": match_edge.group('target'),
                "predicate": match_edge.group('predicate'),
            }
        else:
            raise ValueError(f"Invalid line: {line}")
    return qg
