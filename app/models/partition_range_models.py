from pydantic import BaseModel, Field


class QueryStringResponse(BaseModel):
    """This class is the base class for returning a query string. It has a single attribute (query) which contains the
    (sub-)query string.
    """
    query: str = Field(...,
                       title='Query',
                       description='The query string that can be inserted into a WHERE clause')

    class Config:
        schema_extra = {
            'example': {
                'query': '`year` = 2020 AND `month` = 11 AND `day` BETWEEN 1 AND 13'
            }
        }


class ImpalaPartitionRangeResponse(QueryStringResponse):
    pass


class HivePartitionRangeResponse(QueryStringResponse):
    pass
