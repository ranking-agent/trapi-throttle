# TRAPI Throttle

Proxy service that enforces rate limits on TRAPI endpoints using batching.

## Local Development

### Docker + Management Script

The codebase comes with a zero-dependency python management script that can be used to automate basic local development tasks. Make sure you have docker installed and then run:

```bash
./manage.py dev # starts server accessible at 7830
./manage.py test # run tests
./manage.py coverage # run tests, open coverage report in browser
```

### Without Docker

To run locally without docker you will need python 3.9 as well as Redis installed. After those are installed you can use pip to install the rest of the dependencies:

```bash
pip install -r requirements-lock.txt
pip install -r requirements-test-lock.txt
```

then either `pytest` or `python run.py`.


## Usage

Before requests can be initiated a KP has to be registered. A KP registration object looks like this:
```json
{
  "url": "http://kp1/query",
  "request_qty": 3,
  "request_duration": "0:01"
}
```

This allows 3 requests every minute to the underlying endpoint. Duration can be specified in any of the [Pydantic timedelta formats](https://pydantic-docs.helpmanual.io/usage/types/#datetime-types) including an ISO8601 string or an integer number of seconds.

After the KP is registered, any requests to `/query/{kp_name}` endpoint will be forwarded to the KP with the rate limiting and appropriate buffering applied.


## Architecture

This codebase makes extensive use of Python's asyncio features to handle rate limiting and batching. It uses Redis for buffering requests and responses. This is the general process for how requests are handled:

1. A request comes in as a TRAPI message to the `/query/kp1` endpoint. The request is given a UUID and written to the `kp1:buffer:{UUID}` key in Redis. The request is blocked from returning.

1. When each KP is registered it sets up a batch processing coroutine. This coroutine wakes up when there is a write to the `{kp_id}:buffer:*` key in Redis. 

1. The kp1 coroutine checks for a key `kp1:info:tat`. This key stands for Theoretical Arrival Time, and keeps track of when the next request will be allowed based on the rate limit specified. If it is set, the thread waits for the TAT to elapse. If it is unset the thread simply continues.

1. The coroutine reads all keys stored in `kp1:buffer:*`. The TRAPI messages in the buffer are merged and the keys are deleted. 

1. The coroutine makes a request to the underlying KP and receives a response.

1. The response is split into responses for each initial request. These responses are written to the `kp1:finished:{request_uuid}` keys. 

1. The coroutine updates the TAT key. This key is updated based on the formula `TAT = now + (kp_duration / kp_qty)`. This ensures a smooth set of requests. Then, the coroutine continues waiting for another key in `{kp_id}:buffer:*` to change.

1. The original request coroutine has been waiting for the Redis key `kp1:finished:{request_uuid}`. Once the batch processing coroutine writes the finished request to this key, the request coroutine wakes up.

1. The request coroutine reads the TRAPI message from the `kp1:finished:{request_uuid}` and returns it. The key is deleted from the database.

