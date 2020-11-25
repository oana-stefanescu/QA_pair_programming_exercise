from pydantic import BaseModel, Field


class QueryStringResponse(BaseModel):
    """This response a single field query which is the query string to be used in a WHERE clause.
    """
    query: str = Field(...,
                       title='Query',
                       description='The query string that can be inserted into a WHERE clause')

    class Config:
        schema_extra = {
            'example': {
                'query': '`timestamp` BETWEEN 1606230000 AND 1606327199 AND ((`year` = 2020 AND `month` = 11 AND '
                         '`day` = 24 AND `hour` BETWEEN 15 AND 23) OR (`year` = 2020 AND `month` = 11 AND `day` = 25 '
                         'AND `hour` BETWEEN 0 AND 17))'
            }
        }


class ImpalaPartitionRangeResponse(QueryStringResponse):
    pass


class HivePartitionRangeResponse(QueryStringResponse):
    pass
