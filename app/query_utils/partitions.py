from typing import List, Optional


class RelevantHoursOnDay:
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


class RelevantDaysInMonth:
    """
    Holds the information of the missing days in the month of the start or end date
    Attributes:
         year: the year of the given month
         month: the month
         days: the days of the given months
    """

    def __init__(self, year: int, month: int, days: List[int]):
        self.year = year
        self.month = month
        self.days = days


class RelevantMonthsInYear:
    """
    Holds the information  of the missing months in the year of the start or end date
        Attributes:
         year: the year
         months: the months in the given year
    """

    def __init__(self, year: int, months: List[int]):
        self.year = year
        self.months = months


class RelevantYears:
    """
    Holds the information of the years in between the start and the end date
    Attributes:
        years: the years in between the start date and end date
    """

    def __init__(self, years: List[int]):
        self.years = years


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

    def __init__(self, relevant_hours_of_start_date: Optional[RelevantHoursOnDay] = None,
                 relevant_days_in_start_date_month: Optional[RelevantDaysInMonth] = None,
                 relevant_months_in_start_date_year: Optional[RelevantMonthsInYear] = None,
                 relevant_years: Optional[RelevantYears] = None,
                 relevant_hours_of_end_date: Optional[RelevantHoursOnDay] = None,
                 relevant_days_in_end_date_month: Optional[RelevantDaysInMonth] = None,
                 relevant_months_in_end_date_year: Optional[RelevantMonthsInYear] = None):
        self.relevant_hours_of_start_date = relevant_hours_of_start_date
        self.relevant_days_in_start_date_month = relevant_days_in_start_date_month
        self.relevant_months_in_start_date_year = relevant_months_in_start_date_year
        self.relevant_years = relevant_years
        self.relevant_hours_of_end_date = relevant_hours_of_end_date
        self.relevant_days_in_end_date_month = relevant_days_in_end_date_month
        self.relevant_months_in_end_date_year = relevant_months_in_end_date_year
