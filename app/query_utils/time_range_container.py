from typing import List, Optional
from pydantic.dataclasses import dataclass
from pydantic import validator


@dataclass
class TimeRangeContainer(object):
    """
    Container holding information about the different years, months, days and hours that need to be considered to
    generate the partition queries for a given time range.
    """
    years: List[int]
    months: Optional[List[int]] = None
    days: Optional[List[int]] = None
    hours: Optional[List[int]] = None

    @validator('years')
    def years_must_contain_values(cls, v):
        if not v:
            raise ValueError('A time range container always requires years.')
        return v

    @validator('months')
    def months_is_not_empty(cls, v):
        if v is not None and not v:
            raise ValueError('If months is set, it must contain values.')
        return v

    @validator('days')
    def days_is_not_empty(cls, v):
        if v is not None and not v:
            raise ValueError('If days is set, it must contain values.')
        return v

    @validator('days')
    def months_must_contain_values_if_days_are_set(cls, v, values):
        if v is not None and 'months' in values and values['months'] is None:
            raise ValueError('Months has to contain values if days are set.')
        return v

    @validator('hours')
    def hours_is_not_empty(cls, v):
        if v is not None and not v:
            raise ValueError('If hours is set, it must contain values.')
        return v

    @validator('hours')
    def days_must_contain_values_if_hours_are_set(cls, v, values):
        if v is not None and 'days' in values and values['days'] is None:
            raise ValueError('Days has to contain values if hours are set.')
        return v
