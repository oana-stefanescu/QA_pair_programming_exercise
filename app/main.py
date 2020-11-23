import logging

from fastapi import FastAPI
from fastapi.logger import logger
from prometheus_fastapi_instrumentator import Instrumentator

from .routers import metrics, partition_range


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

# add routers
app.include_router(
    metrics.router,
    prefix='/metrics',
    tags=['prometheus']
)

app.include_router(partition_range.router)


@app.get("/")
def root():
    return {"FastAPI": "is awesome"}


@app.get("/dummy")
def dummy():
    return {"hello": "dummy endpoint"}
