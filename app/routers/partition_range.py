from datetime import datetime

from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import ORJSONResponse

from ..models.partition_range_models import ImpalaPartitionRangeResponse, HivePartitionRangeResponse, QueryStringResponse

router = APIRouter()


def _process_impala_hive_partition_query(start: datetime, end: datetime, cls: type) -> QueryStringResponse:
    """Process a call to the impala / hive endpoint.

    This function will call the function to generate the query string. If further checks if end >= start, if not it will
    raise a HTTPException with status code 422.

    It will response with a sub-class of QueryStringResponse, the return type is given as the cls argument.
    This class (init method) will be called with the result.

    Args:
        start: Start time of the generated time query.
        end: End time of the generated time query.
        cls: The class to be used as the result, it must be a sub-class of QueryStringResponse.

    Returns:
        The QueryStringResponse containing the query partition range for the given start and end.

    Raises:
        HTTPException: With status code 422 if end < start.
    """
    if end < start:
        raise HTTPException(422, detail='end must be >= start date')
    return cls('TODO insert method here')


@router.get('/impala', response_model=ImpalaPartitionRangeResponse, response_class=ORJSONResponse)
async def impala_partition_query(
        start: datetime = Query(...,
                                title='start',
                                description='The start date time the time range should include, only all information '
                                            'up to hour is used, so for example minutes and seconds are ignored.'),
        end: datetime = Query(...,
                              title='end',
                              description='The end date time the time range should include, only all information up '
                                          'to hour is used, so for example minutes and seconds are ignored. Must be '
                                          '>= start.')):
    return _process_impala_hive_partition_query(start, end, ImpalaPartitionRangeResponse)


@router.get('/hive', response_model=HivePartitionRangeResponse, response_class=ORJSONResponse)
async def hive_partition_query(
        start: datetime = Query(...,
                                title='start',
                                description='The start date time the time range should include, only all information '
                                            'up to hour is used, so for example minutes and seconds are ignored.'),
        end: datetime = Query(...,
                              title='end',
                              description='The end date time the time range should include, only all information up '
                                          'to hour is used, so for example minutes and seconds are ignored. Must be '
                                          '>= start.')):
    return _process_impala_hive_partition_query(start, end, HivePartitionRangeResponse)
