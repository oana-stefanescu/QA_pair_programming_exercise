from pytest import fixture
from fastapi.testclient import TestClient

from .. import main


@fixture(scope='session')
def testing_client() -> TestClient:
    client = TestClient(main.app)
    return client
