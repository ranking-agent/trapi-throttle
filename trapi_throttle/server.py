"""Server routes"""
import logging
import pprint

from fastapi import FastAPI
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
    pretty_config = pprint.pformat(
        settings.dict()
    )
    LOGGER.info(f" App Configuration:\n {pretty_config}")

@APP.post('/query/{kp_id}')
async def async_query(
        kp_id: str,
        query: Query,
) -> Query:
    """Queue up a query for batching and return when completed"""
    return query
