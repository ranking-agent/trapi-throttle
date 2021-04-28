from collections import defaultdict
from contextlib import asynccontextmanager, AsyncExitStack
from urllib.parse import urlparse
from functools import partial, wraps
import datetime

from fastapi import FastAPI, Response
from asgiar import ASGIAR

def url_to_host(url):
    # TODO modify ASGIAR to accept a URL instead of a host
    return urlparse(url).netloc

# Store rate limit information in a global
# variable for simplicity
rate_limit_tracker = {}

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
            if url not in rate_limit_tracker:
                rate_limit_tracker[url] = {
                    "start_time" : datetime.datetime.now(),
                    "qty" : 0
                }
            time_since_last_request = \
                datetime.datetime.now() - rate_limit_tracker[url]["start_time"]

            if time_since_last_request > request_duration:
                # Reset rate limit
                rate_limit_tracker[url]["start_time"] = datetime.datetime.now()
                rate_limit_tracker[url]["qty"] = 0

            # Check that we are not above quantity
            rate_limit_tracker[url]["qty"] += 1
            if rate_limit_tracker[url]["qty"] > request_qty:
                return Response(status_code = 429)

            return response

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
