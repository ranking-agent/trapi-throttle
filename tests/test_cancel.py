import asyncio

import aioredis
import pytest

from trapi_throttle.config import settings

async def cancel_me(conn):
    print("executing")
    result = await conn.xread({"test": 0}, block=0)
    print("read", result)
    latest = result[0][1][0][0]
    print("read", await conn.xread({"test": latest}, block=0))


@pytest.mark.asyncio
async def test_cancel():
    conn = await aioredis.Redis.from_url(settings.redis_url, decode_responses=True)
    task = asyncio.create_task(cancel_me(conn))

    conn2 = await aioredis.Redis.from_url(settings.redis_url, decode_responses=True)
    await conn2.xadd("test", {"foo": "bar"})

    await asyncio.sleep(1)

    print("cancelling")
    task.cancel()
    await asyncio.sleep(1)
    try:
        await task
    except asyncio.CancelledError:
        print("cancelled")

    await conn.flushdb()
