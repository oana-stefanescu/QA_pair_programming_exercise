import logging

from fastapi import FastAPI
from fastapi.logger import logger

# make sure logging is also available in production
# see <https://github.com/tiangolo/uvicorn-gunicorn-fastapi-docker/issues/19>
gunicorn_logger = logging.getLogger('gunicorn.error')
logger.handlers = gunicorn_logger.handlers

if __name__ != "main":
    logger.setLevel(gunicorn_logger.level)
else:
    logger.setLevel(logging.DEBUG)

app = FastAPI()


@app.get("/")
def root():
    return {"FastAPI": "is awesome"}


@app.get("/dummy")
def dummy():
    return {"hello": "dummy endpoint"}
