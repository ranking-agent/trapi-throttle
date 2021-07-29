"""Server routes"""
import asyncio
from asyncio.tasks import Task
import copy
import datetime
from functools import wraps
import json
from json.decoder import JSONDecodeError
import logging
import traceback

import aioredis
from fastapi import HTTPException
import httpx
import pydantic
from reasoner_pydantic import Query, Response as ReasonerResponse
from starlette.responses import JSONResponse
import uuid
import uvloop

from .storage import RedisStream
from .trapi import extract_curies, filter_by_curie_mapping
from .utils import get_keys_with_value, log_request, log_response

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


class Throttle():
    """TRAPI Throttle."""

    def __init__(self, *args, redis_url: str = "redis://localhost", **kwargs):
        """Initialize."""
        self.redis_url = redis_url

        # Create a shared redis pool
        self.pool = aioredis.ConnectionPool.from_url(
            self.redis_url,
            decode_responses=True,
        )
        self.redis = aioredis.Redis(connection_pool=self.pool)

        self.workers = dict()
        self.servers = dict()

    @log_errors
    async def process_batch(
            self,
            kp_id: str,
            kp_info: KPInformation,
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

        # Use a new connection because subscribe method alters the connection
        conn = await aioredis.Redis.from_url(self.redis_url, decode_responses=True)

        request_stream = RedisStream(conn, [f"{kp_id}:request"])

        while True:
            # Get everything in the stream or wait for something to show up
            msgs = await request_stream.read(timeout=0)
            if msgs is None:
                continue

            request_id_map = {
                key: msg[0]
                for msg in msgs
                for key in msg[1].keys()
            }

            LOGGER.debug(
                f"Processing batch of size {len(msgs)} for KP {kp_id}"
            )

            request_value_mapping = {
                key: json.loads(value)
                for msg in msgs
                for key, value in msg[1].items()
            }

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
            
            # Remove the requests to be processed from the stream
            for request_id in batch_request_ids:
                await conn.xdel(f"{kp_id}:request", request_id_map[request_id])

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
                    response = await client.post(kp_info.url, json=merged_request_value)
                response.raise_for_status()

                # Parse with reasoner_pydantic to validate
                response = ReasonerResponse.parse_obj(response.json()).dict()
                message = response["message"]

                # Split using the request_curie_mapping
                for request_id, curie_mapping in request_curie_mapping.items():
                    message_filtered = filter_by_curie_mapping(message, curie_mapping)
                    response_values[request_id] = {"message": message_filtered}
            except httpx.RequestError as e:
                for request_id, curie_mapping in request_curie_mapping.items():
                    response_values[request_id] = {
                        "message": "Request Error contacting KP",
                        "error": str(e),
                        "request": log_request(e.request),
                        "status_code" : 502,
                    }
            except httpx.HTTPStatusError as e:
                for request_id, curie_mapping in request_curie_mapping.items():
                    response_values[request_id] = {
                        "message": "Response Error contacting KP",
                        "error": str(e),
                        "request": log_request(e.request),
                        "response": log_response(e.response),
                        "status_code": e.response.status_code,
                    }
            except JSONDecodeError as e:
                for request_id, curie_mapping in request_curie_mapping.items():
                    response_values[request_id] = {
                        "message": "Received bad JSON data from KP",
                        "request": log_request(e.request),
                        "response": log_response(e.response),
                        "error": str(e),
                        "status_code": 502,
                    }
            except pydantic.ValidationError as e:
                for request_id, curie_mapping in request_curie_mapping.items():
                    response_values[request_id] = {
                        "message": "Received non-TRAPI compliant response from KP",
                        "request": log_request(e.request),
                        "response": log_response(e.response),
                        "error": str(e),
                        "status_code" : 502,
                    }

            for request_id, response_value in response_values.items():
                # Write finished value to DB
                await conn.xadd(f"{kp_id}:response:{request_id}", {request_id: json.dumps(response_value)})

            # if request_qty == 0 we don't enforce the rate limit
            if kp_info.request_qty > 0:
                time_remaining_seconds = (tat - datetime.datetime.utcnow()).total_seconds()

                # Wait for TAT
                if time_remaining_seconds > 0:
                    LOGGER.debug(f"Waiting {time_remaining_seconds} seconds")
                    await asyncio.sleep(time_remaining_seconds)

                # Update TAT
                interval = kp_info.request_duration / kp_info.request_qty
                tat = (datetime.datetime.utcnow() + interval)

    async def register_kp(
            self,
            kp_id: str,
            kp_info: KPInformation,
    ):
        """Set KP info and start processing task."""
        info = json.loads(kp_info.json())
        if kp_id in self.servers and self.servers[kp_id] != info:
            raise HTTPException(409, f"{kp_id} already exists")
        self.servers[kp_id] = info

        loop = asyncio.get_event_loop()
        self.workers[kp_id] = loop.create_task(self.process_batch(kp_id, kp_info))

    async def unregister_kp(
            self,
            kp_id: str,
    ):
        """Cancel KP processing task."""
        self.servers.pop(kp_id)
        task: Task = self.workers.pop(kp_id)
        
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            LOGGER.debug(f"Task cancelled: {task}")

    async def query(
            self,
            kp_id: str,
            query: Query,
    ) -> Query:
        """ Queue up a query for batching and return when completed """
        request_id = str(uuid.uuid1())

        # Wait for query to be processed
        conn = await aioredis.Redis.from_url(self.redis_url, decode_responses=True)

        # Insert query into db for processing
        request_stream = RedisStream(conn, [f"{kp_id}:request"])
        await request_stream.add(request_id, query.dict(exclude_unset=True))

        # Wait for response
        response_stream = RedisStream(conn, [f"{kp_id}:response:{request_id}"])
        read = await response_stream.read()

        try:
            output = next(
                value
                for key, value in read[0][1].items()
                if key == request_id
            )
        except StopIteration:
            output = None
        output = json.loads(output)
        status_code = output.pop("status_code", 200)
        return JSONResponse(output, status_code)
