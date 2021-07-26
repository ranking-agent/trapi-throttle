"""Server routes"""
import asyncio
from asyncio.tasks import Task
import copy
import datetime
from functools import wraps
import json
import logging
import traceback
import pprint

from trapi_throttle.utils import gather_dict, get_keys_with_value
from trapi_throttle.trapi import extract_curies, filter_by_curie_mapping
import uuid
import uvloop
import httpx

from pydantic.main import BaseModel
from pydantic import AnyHttpUrl
from trapi_throttle.storage import RedisStream, RedisValue

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from reasoner_pydantic import Query

from .config import settings

import aioredis

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

LOGGER = logging.getLogger(__name__)

APP = FastAPI()

CORS_OPTIONS = dict(
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

APP.add_middleware(
    CORSMiddleware,
    **CORS_OPTIONS,
)


@APP.on_event("startup")
async def startup_event():
    # Print config
    pretty_config = pprint.pformat(
        settings.dict()
    )
    LOGGER.info(f" App Configuration:\n {pretty_config}")

    # Create a shared redis pool
    APP.state.pool = aioredis.ConnectionPool.from_url(
        settings.redis_url,
        decode_responses=True,
    )
    APP.state.redis = aioredis.Redis(connection_pool=APP.state.pool)

    APP.state.workers = dict()
    APP.state.servers = dict()


@APP.on_event('shutdown')
async def shutdown_event():
    await APP.state.redis.close()
    await APP.state.pool.disconnect()


class KPInformation(BaseModel):
    url: AnyHttpUrl
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


@log_errors
async def process_batch(kp_id):
    """Set up a subscriber to process batching"""
    kp_info_db = RedisValue(APP.state.redis, f"{kp_id}:info")
    tat_db = RedisValue(APP.state.redis, f"{kp_id}:tat")

    kp_info = KPInformation.parse_raw(await kp_info_db.get())

    # Initialize the TAT
    #
    # TAT = Theoretical Arrival Time
    # When the next request should be sent
    # to adhere to the rate limit.
    #
    # This is an implementation of the GCRA algorithm
    # More information can be found here:
    # https://dev.to/astagi/rate-limiting-using-python-and-redis-58gk
    await tat_db.set(
        datetime.datetime.utcnow().isoformat()
    )

    # Use a new connection because subscribe method alters the connection
    conn = await aioredis.Redis.from_url(settings.redis_url, decode_responses=True)

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

        now = datetime.datetime.utcnow()
        tat = datetime.datetime.fromisoformat(await tat_db.get())

        time_remaining_seconds = (tat - now).total_seconds()
        # Wait for TAT
        if time_remaining_seconds > 0:
            LOGGER.debug(f"Waiting {time_remaining_seconds}")
            await asyncio.sleep(time_remaining_seconds)

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

        # Make request
        async with httpx.AsyncClient() as client:
            response = await client.post(kp_info.url, json=merged_request_value)
        message = response.json()["message"]

        for request_id, curie_mapping in request_curie_mapping.items():
            # Split using the request_curie_mapping
            message_filtered = filter_by_curie_mapping(message, curie_mapping)

            # Write finished value to DB
            await conn.xadd(f"{kp_id}:response:{request_id}", {request_id: json.dumps(message_filtered)})

        # Update TAT
        interval = kp_info.request_duration / kp_info.request_qty
        new_tat = datetime.datetime.utcnow() + interval
        await tat_db.set(new_tat.isoformat())


@APP.post("/register/{kp_id}")
async def register_kp(
        kp_id: str,
        kp_info: KPInformation,
):
    """Set KP info and start processing task."""
    kp_info_db = RedisValue(APP.state.redis, f"{kp_id}:info")
    await kp_info_db.set(kp_info.json())

    info = json.loads(kp_info.json())
    APP.state.servers[kp_id] = info

    loop = asyncio.get_event_loop()
    APP.state.workers[kp_id] = loop.create_task(process_batch(kp_id))

    return {"status": "created"}


@APP.get("/unregister/{kp_id}")
async def unregister_kp(
        kp_id: str,
):
    """Cancel KP processing task."""
    APP.state.servers.pop(kp_id)
    task: Task = APP.state.workers.pop(kp_id)
    
    task.cancel()

    try:
        await task
    except asyncio.CancelledError:
        LOGGER.debug(f"Task cancelled: {task}")

    return {"status": "removed"}


@APP.post('/{kp_id}/query')
async def query(
        kp_id: str,
        query: Query,
) -> Query:
    """ Queue up a query for batching and return when completed """
    request_id = str(uuid.uuid1())

    # Wait for query to be processed
    # Use a new connection because subscribe method alters the connection
    conn = await aioredis.Redis.from_url(settings.redis_url, decode_responses=True)

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
    output = {"message": json.loads(output)}
    return output


@APP.get("/{kp_id}/meta_knowledge_graph")
async def metakg(kp_id: str):
    url = "/".join(APP.state.servers[kp_id]["url"].split("/")[:-1] + ["meta_knowledge_graph"])
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
    return response.json()
