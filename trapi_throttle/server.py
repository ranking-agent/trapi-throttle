"""Server routes"""
import asyncio
import concurrent.futures
import copy
import datetime
from functools import partial
import logging
import pprint
from trapi_throttle.utils import gather_dict, get_equal_dict_values
from trapi_throttle.trapi import extract_curies, filter_by_curie_mapping
import uuid
import httpx

from pydantic.main import BaseModel
from pydantic import AnyHttpUrl
from starlette.responses import Response
from starlette.background import BackgroundTask
from trapi_throttle.storage import RedisList, RedisValue

from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from reasoner_pydantic import Query

from .config import settings

import aioredis

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
    APP.state.redis = await aioredis.create_redis_pool(
        settings.redis_url,
        encoding="utf-8",
    )


@APP.on_event('shutdown')
async def shutdown_event():
    APP.state.redis.close()
    await APP.state.redis.wait_closed()


class KPInformation(BaseModel):
    url: AnyHttpUrl
    request_qty: int
    request_duration: datetime.timedelta


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
    conn = await aioredis.create_redis(settings.redis_url, encoding="utf-8")

    # Subscribe to changes to the buffer
    kp_buffer_pattern = f"__keyspace@0__:{kp_id}:buffer:*"
    channel, = await conn.psubscribe(kp_buffer_pattern)

    # TODO figure out why we need to sleep here
    # for our tests to pass
    await asyncio.sleep(1)

    # Wait for anything to be added to the buffer
    while await channel.wait_message():
        await channel.get()

        # Check if we actually have work to do
        batch_keys = await APP.state.redis.keys(f"{kp_id}:buffer:*")
        if len(batch_keys) == 0:
            continue

        LOGGER.debug(
            f"Processing batch of size {len(batch_keys)} for KP {kp_id}"
        )

        now = datetime.datetime.utcnow()
        tat = datetime.datetime.fromisoformat(await tat_db.get())

        time_remaining_seconds = (tat - now).total_seconds()
        # Wait for TAT
        if time_remaining_seconds > 0:
            LOGGER.debug(f"Waiting {time_remaining_seconds}")
            await asyncio.sleep(time_remaining_seconds)

        # Process batch
        batch_request_ids = [key.split(':')[-1] for key in batch_keys]

        request_values_db_get = {
            request_id: RedisValue(
                APP.state.redis, f"{kp_id}:buffer:{request_id}").get()
            for request_id in batch_request_ids
        }
        request_value_mapping = await gather_dict(request_values_db_get)

        # Extract a curie mapping from each request
        request_curie_mapping = {
            request_id: extract_curies(request_value["message"]["query_graph"])
            for request_id, request_value in request_value_mapping.items()
        }

        # Find requests that are the same (those that we can merge)
        # This disregards non-matching IDs because the IDs have been
        # removed with the extract_curie method
        request_value_mapping = get_equal_dict_values(request_value_mapping)

        # Filter curie mapping to only include matching requests
        request_curie_mapping = {
            k: v for k, v in request_curie_mapping.items()
            if k in request_value_mapping
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
                if "id" not in node:
                    node["id"] = []
                node["id"].extend(node_curies)

        # Make request
        async with httpx.AsyncClient() as client:
            response = await client.post(kp_info.url, json=merged_request_value)
        message = response.json()["message"]

        response_values_db_set = {}
        request_values_db_del = {}

        for request_id, curie_mapping in request_curie_mapping.items():
            # Split using the request_curie_mapping
            message_filtered = copy.deepcopy(message)
            filter_by_curie_mapping(message_filtered, curie_mapping)

            # Write finished value to DB
            response_values_db_set[request_id] = \
                RedisValue(
                    APP.state.redis, f"{kp_id}:finished:{request_id}"
            ).set({"message": message_filtered})

            # Remove value from buffer
            request_values_db_del[request_id] = \
                RedisValue(APP.state.redis,
                           f"{kp_id}:buffer:{request_id}").delete()

        await gather_dict(response_values_db_set)
        await gather_dict(request_values_db_del)

        # Update TAT
        interval = kp_info.request_duration / kp_info.request_qty
        new_tat = datetime.datetime.utcnow() + interval
        await tat_db.set(new_tat.isoformat())

    # Close redis connection
    conn.close()
    await conn.wait_closed()


@APP.post("/register/{kp_id}")
async def register_kp(
        kp_id: str,
        kp_info: KPInformation,
):
    kp_info_db = RedisValue(APP.state.redis, f"{kp_id}:info")
    await kp_info_db.set(kp_info.json())

    loop = asyncio.get_event_loop()
    loop.create_task(process_batch(kp_id))

    return {"status": "created"}


@APP.post('/query/{kp_id}')
async def query(
        kp_id: str,
        query: Query,
) -> Query:
    """ Queue up a query for batching and return when completed """

    # Insert query into db for processing
    request_id = uuid.uuid1()
    query_input_db = RedisValue(
        APP.state.redis, f"{kp_id}:buffer:{request_id}")
    await query_input_db.set(query.dict())

    # Wait for query to be processed
    # Use a new connection because subscribe method alters the connection
    conn = await aioredis.create_redis(settings.redis_url, encoding="utf-8")
    finished_notification_channel, = await conn.subscribe(
        f"__keyspace@0__:{kp_id}:finished:{request_id}")

    await finished_notification_channel.get()

    # Return output value and remove from database
    query_output_db = RedisValue(
        APP.state.redis, f"{kp_id}:finished:{request_id}")
    output = await query_output_db.get()
    await query_output_db.delete()
    return output
