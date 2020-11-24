from typing import List, Optional


class TimeRange(object):
    """
    Holds information for a certain time range.
    Attributes:
        years: the years of the given time range
        months: the months of the given time range
        days: the days of the given time range
        hours: list of hours of the given time range
    """

    def __init__(self,
                 years: Optional[List[int]] = None,
                 months: Optional[List[int]] = None,
                 days: Optional[List[int]] = None,
                 hours: Optional[List[int]] = None):
        self.years = years
        self.months = months
        self.days = days
        self.hours = hours


class Partitions:
    """
    Container holding all the different partition groups needed to calculate a partition string
    Attributes:
        relevant_hours_of_start_date:
        relevant_days_in_start_date_month:
        relevant_months_in_start_date_year:
        relevant_years:
        relevant_hours_of_end_date:
        relevant_days_in_end_date_month:
        relevant_months_in_end_date_year:
    """

    def __init__(self, relevant_hours_of_start_date: Optional[TimeRange] = None,
                 relevant_days_in_start_date_month: Optional[TimeRange] = None,
                 relevant_months_in_start_date_year: Optional[TimeRange] = None,
                 relevant_years: Optional[TimeRange] = None,
                 relevant_hours_of_end_date: Optional[TimeRange] = None,
                 relevant_days_in_end_date_month: Optional[TimeRange] = None,
                 relevant_months_in_end_date_year: Optional[TimeRange] = None):
        self.relevant_hours_of_start_date = relevant_hours_of_start_date
        self.relevant_days_in_start_date_month = relevant_days_in_start_date_month
        self.relevant_months_in_start_date_year = relevant_months_in_start_date_year
        self.relevant_years = relevant_years
        self.relevant_hours_of_end_date = relevant_hours_of_end_date
        self.relevant_days_in_end_date_month = relevant_days_in_end_date_month
        self.relevant_months_in_end_date_year = relevant_months_in_end_date_year
