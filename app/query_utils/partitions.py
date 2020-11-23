from typing import Dict, List, Optional, Union, Set, Tuple


class Partitions:
    """
    Container holding all the different partition groups needed to calculate a partition string
    Attributes:
        missing_days_in_start_date_month:
        missing_months_in_start_date_year:
        missing_years:
        missing_months_in_end_date_year:
        missing_days_in_end_date_month:
        last_day:
        first_day
    """


class MissingHoursOnDay:
    """
    Holds the information for the first or the last day of a query time range
    Attributes:
        year: the year of the given day
        month: the month of the given day
        day: the day of the given day
        hours: list of hours of the given day
    """

    def __init__(self, year: int, month: int, day: int, hours: List[int]):
        self.year = year
        self.month = month
        self.day = day
        self.hours = hours


class MissingDaysInMonth:
    """
    Holds the information of the missing days in the or start date or end date month
    Attributes:
         year: the year of the given month
         month: the month
         days: the days of the given months
    """

    def __init__(self, year: int, month: int, days: List[int]):
        self.year = year
        self.month = month
        self.days = days


class MissingMonthsInYear:
    """

    """


class MissingYears:
    """

    """
