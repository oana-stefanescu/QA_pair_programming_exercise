import logging

from fastapi import FastAPI, Response
from fastapi.logger import logger
from prometheus_client import multiprocess
from prometheus_client import generate_latest, CollectorRegistry, CONTENT_TYPE_LATEST
from prometheus_fastapi_instrumentator import Instrumentator


APP_NAME = 'Partitioning Service'

# make sure logging is also available in production
# see <https://github.com/tiangolo/uvicorn-gunicorn-fastapi-docker/issues/19>
gunicorn_logger = logging.getLogger('gunicorn.error')
logger.handlers = gunicorn_logger.handlers

if __name__ != "main":
    logger.setLevel(gunicorn_logger.level)
else:
    logger.setLevel(logging.DEBUG)

app = FastAPI(title=APP_NAME)

# create an instrumentator to expose metrics to prometheus
instrumentator = Instrumentator(
    excluded_handlers=["/metrics"],
)
instrumentator.instrument(app)


# This add the prometheus metrics endpoint (in multiprocess mode)
# See <https://github.com/prometheus/client_python/#multiprocess-mode-gunicorn>
@app.get('/metrics')
def metrics():
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    data = generate_latest(registry)
    headers = {'Content-Length': str(len(data))}
    return Response(data, media_type=CONTENT_TYPE_LATEST, headers=headers)


@app.get("/")
def root():
    return {"FastAPI": "is awesome"}


@app.get("/dummy")
def dummy():
    return {"hello": "dummy endpoint"}


@app.get('/sleepy')
def sleepy():
    import time
    from datetime import datetime
    time.sleep(10)
    return {'time': datetime.now()}
