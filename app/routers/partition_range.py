from datetime import datetime, timezone

from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import ORJSONResponse

from ..models.partition_range_models import QueryStringResponse
from ..query_utils.hive_impala_query_builder import generate_timerange_query

router = APIRouter()


def _convert_dt_to_utc(d: datetime) -> datetime:
    """Convert a datetime object to a datetime with timezone set to timezone.utc.

    If the datetime object is a naive object (no tzinfo set) it will be interpreted as being in UTC.
    For example "2020-11-25 17:00:00" will be interpreted as this time in UTC.
    "2020-11-25T17:00:00+01:00" will be interpreted as "2020-11-25 16:00:00+00:00 UTC".

    Args:
        d: The datetime to convert to utc.

    Returns:
        The datetime with tzinfo set to timezone.utc.
    """
    if d.tzinfo is None:
        return d.replace(tzinfo=timezone.utc)
    else:
        return d.astimezone(timezone.utc)


def _process_impala_hive_partition_query(start: datetime, end: datetime, generate_timestamp_clause: bool)\
        -> QueryStringResponse:
    """Process a call to the impala / hive endpoint.

    This function will call the function generate_timerange_query to generate the query string. It further checks if
    end >= start, if not it will raise a HTTPException with status code 422.

    It will response with an instance of QueryStringResponse.

    Args:
        start: Start time of the generated time query.
        end: End time of the generated time query.
        generate_timestamp_clause: If true also generate a timestamp IN-clause for the given start and end.

    Returns:
        The QueryStringResponse containing the query partition range for the given start and end.

    Raises:
        HTTPException: With status code 422 if end < start.
    """
    # make sure to convert all to UTC
    start = _convert_dt_to_utc(start)
    end = _convert_dt_to_utc(end)
    if end < start:
        raise HTTPException(422, detail='end must be >= start date')
    return QueryStringResponse(query=generate_timerange_query(start, end, generate_timestamp_clause))


@router.get('/impala', response_model=QueryStringResponse, response_class=ORJSONResponse)
async def impala_partition_query(
        start: datetime = Query(...,
                                title='start',
                                description='The start date time the time range should include, only all information '
                                            'up to hour is used, so for example minutes and seconds are ignored.'),
        end: datetime = Query(...,
                              title='end',
                              description='The end date time the time range should include, only all information up '
                                          'to hour is used, so for example minutes and seconds are ignored. Must be '
                                          '>= start.'),
        generate_timestamp_clause: bool = Query(True,
                                                title='Timestamp Clause',
                                                description='If true not only create the partition range in the query '
                                                            'but also a timestamp clause based on the start and end '
                                                            'date')):
    return _process_impala_hive_partition_query(start, end, generate_timestamp_clause)


@router.get('/hive', response_model=QueryStringResponse, response_class=ORJSONResponse)
async def hive_partition_query(
        start: datetime = Query(...,
                                title='start',
                                description='The start date time the time range should include, only all information '
                                            'up to hour is used, so for example minutes and seconds are ignored.'),
        end: datetime = Query(...,
                              title='end',
                              description='The end date time the time range should include, only all information up '
                                          'to hour is used, so for example minutes and seconds are ignored. Must be '
                                          '>= start.'),
        generate_timestamp_clause: bool = Query(True,
                                                title='Timestamp Clause',
                                                description='If true not only create the partition range in the query '
                                                            'but also a timestamp clause based on the start and end '
                                                            'date')):
    return _process_impala_hive_partition_query(start, end, generate_timestamp_clause)
