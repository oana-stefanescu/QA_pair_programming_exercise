from typing import List, Optional
from pydantic import BaseModel, validator


class TimeRangeContainer(BaseModel):
    """
    Container holding information about the different years, months, days and hours that need to be considered to
    generate the partition queries for a given time range.
    """
    years: List[int]
    months: Optional[List[int]]
    days: Optional[List[int]]
    hours: Optional[List[int]]


@validator('years')
def years_must_contain_values(cls, v):
    if not len(v):
        raise ValueError('A time range container always requires years')
    return v


@validator('days')
def months__must_contain_values_if_days_are_set(cls, v, values):
    if 'months' in values and values['months'] is None:
        raise ValueError('months has to contain values if days are set')
    return v


@validator('hours')
def days__must_contain_values_if_hours_are_set(cls, v, values):
    if 'days' in values and values['days'] is None:
        raise ValueError('days has to contain values if hours are set')
    return v
