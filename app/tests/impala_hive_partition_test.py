from fastapi.testclient import TestClient


def test_impala_query(testing_client: TestClient):
    response = testing_client.get('/')
    assert response.status_code == 200
    assert response.json() == {"FastAPI": "is awesome"}
