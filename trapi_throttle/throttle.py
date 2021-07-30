"""Server routes"""
import asyncio
from asyncio.queues import QueueEmpty
from asyncio.tasks import Task
import copy
import datetime
from functools import wraps
from json.decoder import JSONDecodeError
import logging
import traceback
from typing import Optional, Union

import httpx
import pydantic
from reasoner_pydantic import Response as ReasonerResponse
import uuid
import uvloop

from .trapi import extract_curies, filter_by_curie_mapping
from .utils import get_keys_with_value

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

LOGGER = logging.getLogger(__name__)


class KPInformation(pydantic.main.BaseModel):
    url: pydantic.AnyHttpUrl
    request_qty: int
    request_duration: datetime.timedelta


def log_errors(fcn):
    @wraps(fcn)
    async def wrapper(*args, **kwargs):
        try:
            return await fcn(*args, **kwargs)
        except Exception as err:
            traceback.print_exc()
            raise
    return wrapper


class ThrottledServer():
    """Throttled server."""

    def __init__(self, id: str, url: str, request_qty: int, request_duration: float, *args, **kwargs):
        """Initialize."""
        self.id = id
        self.worker: Optional[Task] = None
        self.request_queue = asyncio.Queue()
        self.url = url
        self.request_qty = request_qty
        self.request_duration = datetime.timedelta(seconds=request_duration)

    @log_errors
    async def process_batch(
            self,
    ):
        """Set up a subscriber to process batching"""
        # Initialize the TAT
        #
        # TAT = Theoretical Arrival Time
        # When the next request should be sent
        # to adhere to the rate limit.
        #
        # This is an implementation of the GCRA algorithm
        # More information can be found here:
        # https://dev.to/astagi/rate-limiting-using-python-and-redis-58gk
        tat = datetime.datetime.utcnow()

        while True:
            # Get everything in the stream or wait for something to show up
            request_id, payload, response_queue = await self.request_queue.get()
            request_value_mapping = {
                request_id: payload
            }
            response_queues = {
                request_id: response_queue
            }
            while True:
                try:
                    request_id, payload, response_queue = self.request_queue.get_nowait()
                except QueueEmpty:
                    break
                request_value_mapping[request_id] = payload
                response_queues[request_id] = response_queue

            LOGGER.debug(
                f"Processing batch of size {len(request_value_mapping)} for KP {self.id}"
            )

            # Extract a curie mapping from each request
            request_curie_mapping = {
                request_id: extract_curies(request_value["message"]["query_graph"])
                for request_id, request_value in request_value_mapping.items()
            }

            # Find requests that are the same (those that we can merge)
            # This disregards non-matching IDs because the IDs have been
            # removed with the extract_curie method
            first_value = next(iter(request_value_mapping.values()))
            batch_request_ids = get_keys_with_value(
                request_value_mapping, first_value)
            
            # Re-queue the un-selected requests
            for request_id in request_value_mapping:
                if request_id not in batch_request_ids:
                    await self.request_queue.put((
                        request_id,
                        request_value_mapping[request_id],
                        response_queues[request_id],
                    ))

            request_value_mapping = {
                k: v for k, v in request_value_mapping.items()
                if k in batch_request_ids
            }

            # Filter curie mapping to only include matching requests
            request_curie_mapping = {
                k: v for k, v in request_curie_mapping.items()
                if k in batch_request_ids
            }

            # Pull first value from request_value_mapping
            # to use as a template for our merged request
            merged_request_value = copy.deepcopy(
                next(iter(request_value_mapping.values()))
            )

            # Remove qnode ids
            for qnode in merged_request_value["message"]["query_graph"]["nodes"].values():
                qnode.pop("ids", None)

            # Update merged request using curie mapping
            for curie_mapping in request_curie_mapping.values():
                for node_id, node_curies in curie_mapping.items():
                    node = merged_request_value["message"]["query_graph"]["nodes"][node_id]
                    if "ids" not in node:
                        node["ids"] = []
                    node["ids"].extend(node_curies)

            response_values = dict()
            try:
                # Make request
                async with httpx.AsyncClient() as client:
                    response = await client.post(self.url, json=merged_request_value)
                response.raise_for_status()

                # Parse with reasoner_pydantic to validate
                response = ReasonerResponse.parse_obj(response.json()).dict()
                message = response["message"]

                # Split using the request_curie_mapping
                for request_id, curie_mapping in request_curie_mapping.items():
                    message_filtered = filter_by_curie_mapping(message, curie_mapping)
                    response_values[request_id] = {"message": message_filtered}
            except (
                httpx.RequestError,
                httpx.HTTPStatusError,
                JSONDecodeError,
                pydantic.ValidationError,
            ) as e:
                for request_id, curie_mapping in request_curie_mapping.items():
                    response_values[request_id] = e

            for request_id, response_value in response_values.items():
                # Write finished value to DB
                await response_queues[request_id].put(response_value)

            # if request_qty == 0 we don't enforce the rate limit
            if self.request_qty > 0:
                time_remaining_seconds = (tat - datetime.datetime.utcnow()).total_seconds()

                # Wait for TAT
                if time_remaining_seconds > 0:
                    LOGGER.debug(f"Waiting {time_remaining_seconds} seconds")
                    await asyncio.sleep(time_remaining_seconds)

                # Update TAT
                interval = self.request_duration / self.request_qty
                tat = (datetime.datetime.utcnow() + interval)

    async def __aenter__(
            self,
    ):
        """Set KP info and start processing task."""
        loop = asyncio.get_event_loop()
        self.worker = loop.create_task(self.process_batch())

        return self

    async def __aexit__(
            self,
            *args,
    ):
        """Cancel KP processing task."""
        task: Task = self.worker
        self.worker = None
        
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            LOGGER.debug(f"Task cancelled: {task}")

    async def query(
            self,
            query: dict,
    ) -> dict:
        """ Queue up a query for batching and return when completed """
        if self.worker is None:
            raise RuntimeError("Cannot send a request until a worker is running - enter the context")

        request_id = str(uuid.uuid1())
        response_queue = asyncio.Queue()

        # Queue query for processing
        await self.request_queue.put((request_id, query, response_queue))

        # Wait for response
        output: Union[dict, Exception] = await response_queue.get()

        if isinstance(output, Exception):
            raise output

        return output

    
class DuplicateError(Exception):
    """Duplicate KP."""


class Throttle():
    """TRAPI Throttle."""

    def __init__(self, *args, **kwargs):
        """Initialize."""
        self.servers: dict[str, ThrottledServer] = dict()

    async def register_kp(
            self,
            kp_id: str,
            kp_info: dict,
    ):
        """Set KP info and start processing task."""
        if kp_id in self.servers:
            raise DuplicateError(f"{kp_id} already exists")
        self.servers[kp_id] = ThrottledServer(kp_id, **kp_info)
        await self.servers[kp_id].__aenter__()

    async def unregister_kp(
            self,
            kp_id: str,
    ):
        """Cancel KP processing task."""
        await self.servers.pop(kp_id).__aexit__()

    async def query(
            self,
            kp_id: str,
            query: dict,
    ) -> dict:
        """ Queue up a query for batching and return when completed """
        return await self.servers[kp_id].query(query)
