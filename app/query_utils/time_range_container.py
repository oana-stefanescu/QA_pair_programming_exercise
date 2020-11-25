from typing import List, Optional
from dataclasses import dataclass


@dataclass()
class TimeRangeContainer(object):
    """
    Container holding information about the different years, months, days and hours that need to be considered to
    generate the partition queries for a given time range.
    """
    years: List[int]
    months: Optional[List[int]]
    days: Optional[List[int]]
    hours: Optional[List[int]]
