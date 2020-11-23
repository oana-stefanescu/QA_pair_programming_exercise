import logging
import os
from typing import List, Dict, Union

from fastapi import FastAPI
from fastapi.logger import logger
from prometheus_fastapi_instrumentator import Instrumentator

from .routers import metrics


APP_NAME = 'Partitioning Service'

# make sure logging is also available in production
# see <https://github.com/tiangolo/uvicorn-gunicorn-fastapi-docker/issues/19>
gunicorn_logger = logging.getLogger('gunicorn.error')
logger.handlers = gunicorn_logger.handlers

if __name__ != "main":
    logger.setLevel(gunicorn_logger.level)
else:
    logger.setLevel(logging.DEBUG)

# get the app root directory for reading files, if the environment variable is not defined use ./
# this is the directory in which the root directory (containing the VERSION file) is located
app_root: str = os.environ.get('APP_ROOT', './')

version_file_path: str = os.path.join(app_root, 'VERSION')

with open(version_file_path) as version_file:
    version: str = version_file.readline()
    version = version.strip()
    if not version.startswith('v'):
        version = 'v' + version

description_file_path: str = os.path.join(app_root, 'app_description.md')

with open(description_file_path) as description_file:
    description: str = description_file.read()

tags_metadata: List[Dict[str, Union[str, Dict[str, str]]]] = [
    {
        'name': 'prometheus',
        'description': 'The prometheus endpoint, returns the prometheus metrics format. It uses the metrics from '
                       'prometheus-fastapi-instrumentator.',
        'externalDocs': {
            'description': 'prometheus-fastapi-instrumentator',
            'url': 'https://pypi.org/project/prometheus-fastapi-instrumentator/',
        }
    }
]

app = FastAPI(title=APP_NAME,
              version=version,
              description=description,
              openapi_tags=tags_metadata,
)

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


@app.get("/")
def root():
    return {"FastAPI": "is awesome"}


@app.get("/dummy")
def dummy():
    return {"hello": "dummy endpoint"}
