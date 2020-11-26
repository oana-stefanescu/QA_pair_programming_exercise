from datetime import datetime, timezone, timedelta

from fastapi.testclient import TestClient


def _test_utc_only(testing_client: TestClient, endpoint: str):
    """This runs basically the tests we have as unit tests, but tests the endpoint.
    """
    expected = "`timestamp` BETWEEN 1494687600 AND 1562587200" \
               " AND " \
               "(" \
               "(`year` = 2017 AND `month` = 5 AND `day` = 13 AND `hour` BETWEEN 15 AND 23)" \
               " OR " \
               "(`year` = 2017 AND `month` = 5 AND `day` BETWEEN 14 AND 31)" \
               " OR " \
               "(`year` = 2017 AND `month` BETWEEN 6 AND 12)" \
               " OR " \
               "(`year` = 2018)" \
               " OR " \
               "(`year` = 2019 AND `month` BETWEEN 1 AND 6)" \
               " OR " \
               "(`year` = 2019 AND `month` = 7 AND `day` BETWEEN 1 AND 7)" \
               " OR " \
               "(`year` = 2019 AND `month` = 7 AND `day` = 8 AND `hour` BETWEEN 0 AND 12)" \
               ")"
    start_time = datetime(year=2017, month=5, day=13, hour=15, tzinfo=timezone.utc)
    end_time = datetime(year=2019, month=7, day=8, hour=12, tzinfo=timezone.utc)
    parameters = {'start': start_time, 'end': end_time}
    response = testing_client.get(endpoint, params=parameters)
    assert response.status_code == 200
    assert response.json() == {'query': expected}

    expected = "`timestamp` BETWEEN 1494712800 AND 1494799199 AND " \
               "(" \
               "(`year` = 2017 AND `month` = 5 AND `day` = 13 AND `hour` BETWEEN 22 AND 23)" \
               " OR " \
               "(`year` = 2017 AND `month` = 5 AND `day` = 14 AND `hour` BETWEEN 0 AND 21)" \
               ")"
    start_time = datetime(year=2017, month=5, day=13, hour=22, tzinfo=timezone.utc)
    end_time = datetime(year=2017, month=5, day=14, hour=21, minute=59, second=59, tzinfo=timezone.utc)
    parameters = {'start': start_time, 'end': end_time}
    response = testing_client.get(endpoint, params=parameters)
    assert response.status_code == 200
    assert response.json() == {'query': expected}

    # test that it works correctly when setting the timestamp clause to false
    expected = "(" \
               "(`year` = 2017 AND `month` = 5 AND `day` = 13 AND `hour` BETWEEN 22 AND 23)" \
               " OR " \
               "(`year` = 2017 AND `month` = 5 AND `day` = 14 AND `hour` BETWEEN 0 AND 21)" \
               ")"
    parameters['generate_timestamp_clause'] = False
    response = testing_client.get(endpoint, params=parameters)
    assert response.status_code == 200
    assert response.json() == {'query': expected}


def test_impala_query_with_utc(testing_client: TestClient):
    _test_utc_only(testing_client, '/impala')


def test_hive_query_with_utc(testing_client: TestClient):
    _test_utc_only(testing_client, '/hive')


def _test_with_offset(testing_client: TestClient, endpoint: str):
    """Test that if we use an offset it is converted correctly
    """
    expected = "`timestamp` BETWEEN 1494712800 AND 1494799199 AND " \
               "(" \
               "(`year` = 2017 AND `month` = 5 AND `day` = 13 AND `hour` BETWEEN 22 AND 23)" \
               " OR " \
               "(`year` = 2017 AND `month` = 5 AND `day` = 14 AND `hour` BETWEEN 0 AND 21)" \
               ")"
    start_time = datetime(year=2017, month=5, day=13, hour=23, tzinfo=timezone(timedelta(hours=1)))
    end_time = datetime(year=2017, month=5, day=14, hour=23, minute=59, second=59, tzinfo=timezone(timedelta(hours=2)))
    parameters = {'start': start_time, 'end': end_time}
    response = testing_client.get(endpoint, params=parameters)
    assert response.status_code == 200
    assert response.json() == {'query': expected}


def test_impala_query_with_offset(testing_client: TestClient):
    _test_with_offset(testing_client, '/impala')


def test_hive_query_with_offset(testing_client: TestClient):
    _test_with_offset(testing_client, '/hive')


def _test_invalid_start_end(testing_client: TestClient, endpoint: str):
    """Test that if end is before start a 422 response is returned.
    """
    start_time = datetime(year=2017, month=5, day=14, hour=23, tzinfo=timezone.utc)
    end_time = datetime(year=2017, month=5, day=14, hour=22, minute=59, second=59, tzinfo=timezone.utc)
    parameters = {'start': start_time, 'end': end_time}
    response = testing_client.get(endpoint, params=parameters)
    assert response.status_code == 422
    assert response.json() == {'detail': 'end date can not be before start date'}


def test_impala_query_invalid_start_end(testing_client: TestClient):
    _test_invalid_start_end(testing_client, '/impala')


def test_hive_query_invalid_start_end(testing_client: TestClient):
    _test_invalid_start_end(testing_client, '/hive')
