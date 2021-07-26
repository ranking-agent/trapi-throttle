"""Storage."""
from abc import ABC
import json

from aioredis import Redis



def mapd(f, d):
    """ Map function over dictionary values """
    return {k: f(v) for k, v in d.items()}


class RedisStream(ABC):
    """Redis value."""

    def __init__(self, r: Redis, streams: list[str]):
        self.r = r
        self.streams = streams
        self.latest = "0"

    async def add(self, key: str, value: any):
        await self.r.xadd(self.streams[0], {key: json.dumps(value)})

    async def read(self, timeout=0):
        stream_results = await self.r.xread(
            {
                stream: self.latest
                for stream in self.streams
            },
            block=timeout,
        )
        for stream, stream_result in stream_results:
            return stream_result

    async def range(self, **kwargs):
        items = await self.r.xrange(self.stream, **kwargs)

    async def delete(self):
        await self.r.delete(self.key)

    async def expire(self, when: int):
        await self.r.expire(self.key, when)


class RedisValue(ABC):
    """Redis value."""

    def __init__(self, r, key: str):
        self.r = r
        self.key = key

    async def set(self, v: any):
        await self.r.set(self.key, json.dumps(v))

    async def get(self):
        v = await self.r.get(self.key)
        return json.loads(v)

    async def delete(self):
        await self.r.delete(self.key)

    async def expire(self, when: int):
        await self.r.expire(self.key, when)


class RedisHash(RedisValue):
    """Redis hash."""

    async def get(self):
        v = await self.r.hgetall(self.key)
        return mapd(json.loads, v)

    async def set(self, v: dict):
        await self.r.delete(self.key)
        if not len(v):
            return
        await self.r.hset(
            self.key,
            mapping=mapd(json.dumps, v)
        )

    async def get_val(self):
        v = await self.r.hget(self.key)
        return json.load(v)

    async def merge(self, v: dict):
        if not len(v):
            return
        await self.r.hset(
            self.key,
            mapping=mapd(json.dumps, v)
        )


class RedisList(RedisValue):
    """Redis list."""

    async def get(self):
        v = await self.r.lrange(self.key, 0, -1)
        return map(json.loads, v)

    async def set(self, v: list[any]):
        # Clear
        await self.r.delete(self.key)
        await self.r.push(**map(json.dumps, v))

    async def append(self, v):
        await self.r.lpush(self.key, json.dumps(v))
