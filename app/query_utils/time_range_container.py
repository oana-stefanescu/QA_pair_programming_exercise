from typing import List, Optional


class TimeRangeContainer(object):
    """
    Container holding information about the different years, months, days and hours that need to be considered to
    generate the partition queries for a given time range.

    Attributes:
        years: the years of the given time range
        months: the months of the given time range
        days: the days of the given time range
        hours: list of hours of the given time range
    """

    def __init__(self, years: Optional[List[int]] = None,
                 months: Optional[List[int]] = None,
                 days: Optional[List[int]] = None,
                 hours: Optional[List[int]] = None):
        self.years = years
        self.months = months
        self.days = days
        self.hours = hours
