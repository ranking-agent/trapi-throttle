"""Server routes"""
import asyncio
import datetime
from functools import wraps
import logging
import traceback
import pprint
from trapi_throttle.throttle import Throttle

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import httpx
import pydantic
from reasoner_pydantic import Query
import uvloop

from .config import settings

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
    APP.throttle = Throttle(redis_url=settings.redis_url)


@APP.on_event('shutdown')
async def shutdown_event():
    pass


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


@APP.post("/register/{kp_id}")
async def register_kp(
        kp_id: str,
        kp_info: KPInformation,
):
    """Set KP info and start processing task."""
    await APP.throttle.register_kp(kp_id, kp_info)

    return {"status": "created"}


@APP.get("/unregister/{kp_id}")
async def unregister_kp(
        kp_id: str,
):
    """Cancel KP processing task."""
    await APP.throttle.unregister_kp(kp_id)

    return {"status": "removed"}


@APP.post('/{kp_id}/query')
async def query(
        kp_id: str,
        query: Query,
) -> Query:
    """ Queue up a query for batching and return when completed """
    return await APP.throttle.query(kp_id, query)


@APP.get("/{kp_id}/meta_knowledge_graph")
async def metakg(kp_id: str):
    url = "/".join(APP.throttle.servers[kp_id]["url"].split("/")[:-1] + ["meta_knowledge_graph"])
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
    return response.json()
