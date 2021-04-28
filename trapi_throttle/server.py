"""Server routes"""
import datetime
import logging
import pprint

from pydantic.main import BaseModel
from pydantic import HttpUrl
from trapi_throttle.storage import RedisHash, RedisList

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from reasoner_pydantic import Query

from .config import settings

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
async def print_config():
    # Print config
    pretty_config = pprint.pformat(
        settings.dict()
    )
    LOGGER.info(f" App Configuration:\n {pretty_config}")

    # Initialize Redis
    if settings.redis_url == "redis://fakeredis:6379/0":
        import fakeredis.aioredis
        APP.state.redis = await fakeredis.aioredis.create_redis_pool(
            encoding="utf-8",
        )
    else:
        import aioredis
        APP.state.redis = await aioredis.create_redis_pool(
            settings.redis_url,
            encoding="utf-8",
        )

@APP.on_event('shutdown')
async def shutdown_event():
    APP.state.redis.close()
    await APP.state.redis.wait_closed()

class KPInformation(BaseModel):
    url: HttpUrl
    request_qty: int
    request_duration: datetime.timedelta

@APP.post("/register/{kp_id}")
async def register_kp(
        kp_id: str,
        kp_info: KPInformation,
        request: Request,
):
    kp_info_db = RedisHash(request.app.state.redis, f"{kp_id}:info")
    await kp_info_db.set(kp_info.dict())

    # Set up a subscriber

@APP.post('/query/{kp_id}')
async def query(
        kp_id: str,
        query: Query,
        request: Request
) -> Query:
    """ Queue up a query for batching and return when completed """

    buffer = RedisList(request.app.state.redis, f"{kp_id}:buffer")
    await buffer.append(query.dict())

    return query
