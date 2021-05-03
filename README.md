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

To run locally without docker, use the following commands:

```bash
pip install -r requirements-lock.txt
pip install -r requirements-test-lock.txt
```

then either `pytest` or `python run.py`.

