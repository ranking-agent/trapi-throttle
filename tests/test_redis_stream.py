"""Test Redis stream."""
import asyncio

import aioredis
import pytest

from trapi_throttle.config import settings
from trapi_throttle.storage import RedisStream


@pytest.mark.asyncio
async def test_redis_stream():
    """Test Redis stream."""
    write_stream = RedisStream(
        await aioredis.Redis.from_url(settings.redis_url),
        ["mystream"],
    )
    read_stream = RedisStream(
        await aioredis.Redis.from_url(settings.redis_url),
        ["mystream"],
    )
    await write_stream.add("mykey", {"things": "stuff"})
    print("Added")
    loop = asyncio.get_event_loop()
    loop.create_task(read_stream.read())
    await asyncio.sleep(1)
    await write_stream.add("myotherkey", {"things": "other stuff"})
    print("Added")
    await asyncio.sleep(2)
