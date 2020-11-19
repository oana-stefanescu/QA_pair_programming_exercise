from fastapi import APIRouter, Response
from prometheus_client import multiprocess
from prometheus_client import generate_latest, CollectorRegistry, CONTENT_TYPE_LATEST

router = APIRouter()


# This adds the prometheus metrics endpoint (in multiprocess mode)
# See <https://github.com/prometheus/client_python/#multiprocess-mode-gunicorn>
@router.get('/')
def metrics():
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    data = generate_latest(registry)
    headers = {'Content-Length': str(len(data))}
    return Response(data, media_type=CONTENT_TYPE_LATEST, headers=headers)
