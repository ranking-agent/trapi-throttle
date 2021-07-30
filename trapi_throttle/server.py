"""Server routes"""
import asyncio
import datetime
from functools import wraps
from json.decoder import JSONDecodeError
import logging
import traceback
import pprint

from fastapi import FastAPI
from fastapi.exceptions import HTTPException
from fastapi.middleware.cors import CORSMiddleware
import httpx
import pydantic
from reasoner_pydantic import Query
from starlette.responses import JSONResponse
import uvloop

from .config import settings
from .throttle import DuplicateError, Throttle
from .utils import log_request, log_response

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
    APP.throttle = Throttle()


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
    try:
        await APP.throttle.register_kp(kp_id, kp_info)
    except DuplicateError as err:
        raise HTTPException(409, str(err))

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
    try:
        return JSONResponse(await APP.throttle.query(kp_id, query))
    except httpx.RequestError as e:
        return JSONResponse({
            "message": "Request Error contacting KP",
            "request": log_request(e.request),
            "error": str(e),
        }, 502)
    except httpx.HTTPStatusError as e:
        return JSONResponse({
            "message": "Response Error contacting KP",
            "request": log_request(e.request),
            "response": log_response(e.response),
            "error": str(e),
        }, e.response.status_code)
    except JSONDecodeError as e:
        return JSONResponse({
            "message": "Received bad JSON data from KP",
            "request": log_request(e.request),
            "response": log_response(e.response),
            "error": str(e),
        }, 502)
    except pydantic.ValidationError as e:
        return JSONResponse({
            "message": "Received non-TRAPI compliant response from KP",
            "request": log_request(e.request),
            "response": log_response(e.response),
            "error": str(e),
        }, 502)


@APP.get("/{kp_id}/meta_knowledge_graph")
async def metakg(kp_id: str):
    url = "/".join(APP.throttle.servers[kp_id].url.split("/")[:-1] + ["meta_knowledge_graph"])
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
    return response.json()
