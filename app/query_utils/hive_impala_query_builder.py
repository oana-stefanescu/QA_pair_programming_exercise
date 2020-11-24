from calendar import monthrange
import time
from datetime import datetime
import pytz
from dateutil import parser
from dateutil.relativedelta import relativedelta

from app.query_utils.date_functions import Date
from app.query_utils.partitions import *


def generate_timerange_query(start: datetime, end: datetime) -> str:
    """
    Generates the timerange query for partitioning that suits both hive and impala queries
    :param start: the startdate in UTC
    :param end: the enddate in UTC
    :return: the partition query string
    """
    assert start.tzinfo == pytz.utc and end.tzinfo == pytz.utc
    assert Date.is_first_date_time_greater(start, end)


class PartitionQueryBuilder:
    """
    Generates the hive partition string for the given start and end dates.
    """

    def __init__(self, start_date: datetime, end_date: datetime):
        """
        Validate the given date strings and convert it to UTC

        :param startdate: The startdate in UTC
        :param enddate:  The enddate in UTC
        """
        self.start_date = start_date
        self.end_date = end_date
        self.start_hour = self._get_start_hour()
        self.start_day = self._get_start_day()
        self.start_month = self._get_start_month()
        self.start_year = self._get_start_year()
        self.end_hour = self._get_end_hour()
        self.end_day = self._get_end_day()
        self.end_month = self._get_end_month()
        self.end_year = self._get_end_year()
        self.partitions = Partitions(relevant_hours_of_start_date=self._get_relevant_hours_of_start_date(),
                                     relevant_days_in_start_date_month=self._get_relevant_days_in_start_date_month(),
                                     relevant_months_in_start_date_year=self._get_relevant_months_in_start_date_year(),
                                     relevant_years=self._get_relevant_years(),
                                     relevant_hours_of_end_date=self._get_relevant_hours_of_end_date(),
                                     relevant_days_in_end_date_month=self._get_relevant_days_in_end_date_month(),
                                     relevant_months_in_end_date_year=self._get_relevant_months_in_end_date_year())

    @staticmethod
    def _get_date_part_as_int(date_time, date_part):
        return int(date_time.strftime(date_part))

    def _get_start_year(self):
        return self._get_date_part_as_int(self.start_date, '%Y')

    def _get_end_year(self):
        return self._get_date_part_as_int(self.end_date, '%Y')

    def _get_start_month(self):
        return self._get_date_part_as_int(self.start_date, '%m')

    def _get_end_month(self):
        return self._get_date_part_as_int(self.end_date, '%m')

    def _get_start_day(self):
        return self._get_date_part_as_int(self.start_date, '%d')

    def _get_end_day(self):
        return self._get_date_part_as_int(self.end_date, '%d')

    def _get_start_hour(self):
        return self._get_date_part_as_int(self.start_date, '%H')

    def _get_end_hour(self):
        return self._get_date_part_as_int(self.end_date, '%H')

    def _remove_duplicate_month(self, months, date):
        """
        If the year of the startdate is the same as the year of the enddate, the methods
        `_get_missing_months_in_end_date_year` and `__getMissingMonthsInStartDateYear`
        create more partitions than are actually needed.
        Those duplicate monthts are removed here.

        :param months: List of months as integers
        :param date: The calculated date for the diff
        :return: List of months as integers
        """
        if self.start_year == self.end_year:
            month = self._get_date_part_as_int(date, '%m')
            if month in months:
                months.remove(month)

        return months

    def _remove_duplicated_day(self, days, date):
        """
        If the year of the startdate and the enddate are in the same months
        `_get_missing_days_in_start_date_month` and `_get_missing_days_in_end_date_month`
        create more partitions than are actually needed.
        Those duplicate days are removed here.

        :param months: List of days as integers
        :param date: The calculated date for the diff
        :return: List of days as integers
        """
        if (self.end_year == self.start_year
                and
                self.end_month == self.start_month):
            day = self._get_date_part_as_int(date, '%d')
            if day in days:  # pragma: no cover
                days.remove(day)

        return days

    def _get_relevant_hours_of_start_date(self) -> TimeRange:
        """
        Calculates the hours of the first day.

        :return: Missing hours on the first days
        """
        if not (self.start_day == self.end_day
                and
                self.start_month == self.end_month
                and
                self.start_year == self.end_year):
            hours = []
            for hour in range(self.start_hour, 24):
                hours.append(hour)
            return TimeRange(years=[self.start_year], months=[self.start_month], days=[self.start_day], hours=hours)

    def _get_relevant_hours_of_end_date(self) -> TimeRange:
        """
        Calculates the missing hours for the enddate.

        :return: Missing hours on the last day
        """

        if self.start_day == self.end_day and self.start_month == self.end_month and self.start_year == self.end_year:
            first_hour = self.start_hour
        else:
            first_hour = 0

        hours = []
        for hour in range(first_hour, self.end_hour + 1):
            hours.append(hour)
        return TimeRange(years=[self.end_year], months=[self.end_month], days=[self.end_day], hours=hours)

    def _get_relevant_years(self) -> TimeRange:
        """
        If there is more than one year time difference
        add the complete missing years to the partitions.

        :return: Missing years between the start and the end date
        """
        if relativedelta(self.end_date, self.start_date).years > 0:
            years = []
            for missing_year in range(self.start_year, self.end_year):
                if missing_year != self.start_year:
                    years.append(missing_year)
            if len(years) > 0:  # pragma: no cover
                return TimeRange(years=years)

    def _get_relevant_days_in_start_date_month(self) -> TimeRange:
        """
        Calculates the missing day partitions for the month of the startdate.

        :return: Missing days in th start date month
        """
        last_day_of_month = parser.parse(
            "{0}-{1}-{2}T23:59:59+00:00".format(
                self.start_year,
                self.start_month,
                monthrange(self.start_year, self.start_month)[1]
            )
        )

        if Date.is_first_date_time_greater(last_day_of_month, self.end_date):
            date_for_diff = self.end_date
        else:
            date_for_diff = last_day_of_month

        if (relativedelta(date_for_diff, self.start_date).days > 0 or
                relativedelta(date_for_diff, self.start_date).hours == 23):
            days = []
            for missing_day in range(
                    self.start_day + 1,
                    self._get_date_part_as_int(date_for_diff, "%d") + 1
            ):
                days.append(missing_day)

            days = self._remove_duplicated_day(days, self.end_date)
            if len(days) > 0:
                return TimeRange(years=[self.start_year], months=[self.start_month], days=days)

    def _get_relevant_months_in_start_date_year(self) -> TimeRange:
        """
        Calculates the missing months for the year of the startdate.

        :return: Missing months in the start date year
        """
        last_day_of_year = parser.parse(
            "{0}-12-31T23:59:59+00:00".format(
                self.start_year
            )
        )

        if Date.is_first_date_time_greater(last_day_of_year, self.end_date):
            date_for_diff = self.end_date
        else:
            date_for_diff = last_day_of_year

        if relativedelta(date_for_diff, self.start_date).months > 0:
            months = []

            compare_month = int(date_for_diff.strftime("%m"))
            if compare_month == 12:
                compare_month = compare_month + 1

            for missing_month in range(self.start_month + 1, compare_month):
                months.append(missing_month)

            months = self._remove_duplicate_month(months, self.start_date)
            if len(months) > 0:
                return TimeRange(years=[self.start_year], months=months)

    def _get_relevant_days_in_end_date_month(self) -> TimeRange:
        """
        Calculates the missing days partitions for the month of the enddate.

        :return: Missing days in the end date month
        """
        first_day_of_end_date_month = parser.parse(
            "{0}-{1}-01T00:00:00+00:00".format(
                self.end_year,
                self.end_month
            )
        )

        if Date.is_first_date_time_greater(self.start_date, first_day_of_end_date_month):
            date_for_diff = self.start_date
        else:
            date_for_diff = first_day_of_end_date_month

        if (relativedelta(self.end_date, date_for_diff).months > 0 or
                relativedelta(self.end_date, date_for_diff).days > 0):
            days = []
            for missing_days in range(int(date_for_diff.strftime("%d")), self.end_day):
                days.append(missing_days)

            days = self._remove_duplicated_day(days, self.start_date)
            if len(days) > 0:
                return TimeRange(years=[self.end_year], months=[self.end_month], days=days)

    def _get_relevant_months_in_end_date_year(self) -> TimeRange:
        """
        Calculates the missing months for the year of the enddate.

        :return: Missing months in the end date year
        """
        first_day_of_end_date_year = parser.parse(
            "{0}-01-01T00:00:00+00:00".format(self.end_year)
        )

        if Date.is_first_date_time_greater(self.start_date, first_day_of_end_date_year):
            date_for_diff = self.start_date
        else:
            date_for_diff = first_day_of_end_date_year

        if relativedelta(self.end_date, date_for_diff).months > 0:
            months = []
            for missing_month in range(
                    self._get_date_part_as_int(date_for_diff, '%m'),
                    self.end_month):
                months.append(missing_month)

            months = self._remove_duplicate_month(months, self.start_date)
            if len(months) > 0:
                return TimeRange(years=[self.end_year], months=months)

    @staticmethod
    def _build_where_clause_for_key(key: str, values: Optional[List[int]] = None) -> Optional[str]:
        if not values:
            return None
        if len(values) == 1:
            return "`{0}` = {1}".format(key, str(values[0]))
        else:
            return "`{0}` BETWEEN {1} AND {2}".format(key, str(min(values)), str(max(values)))

    def _build_query_from_partition(self, time_range: Optional[TimeRange] = None) -> Optional[str]:
        if time_range is None:
            return None
        where_clauses = [
            self._build_where_clause_for_key('year', time_range.years),
            self._build_where_clause_for_key('month', time_range.months),
            self._build_where_clause_for_key('day', time_range.days),
            self._build_where_clause_for_key('hour', time_range.hours)
        ]
        # filter out None values
        where_clauses = list(filter(lambda x: x is not None, where_clauses))
        # combine to full where clause
        full_where_clause = '({0})'.format(' AND '.join(where_clauses))
        return full_where_clause

    def build_partition_filter(self):
        partition_where_clauses = [
            self._build_query_from_partition(self.partitions.relevant_hours_of_start_date),
            self._build_query_from_partition(self.partitions.relevant_days_in_start_date_month),
            self._build_query_from_partition(self.partitions.relevant_months_in_start_date_year),
            self._build_query_from_partition(self.partitions.relevant_years),
            self._build_query_from_partition(self.partitions.relevant_months_in_end_date_year),
            self._build_query_from_partition(self.partitions.relevant_days_in_end_date_month),
            self._build_query_from_partition(self.partitions.relevant_hours_of_end_date)
        ]
        # filter out None values
        partition_where_clauses = list(filter(lambda x: x is not None, partition_where_clauses))
        # remove duplicates
        # TODO: how can this happen?
        partition_where_clauses = list(set(partition_where_clauses))
        # combine to full partitioning clause
        full_partitioning_clause = '({0})'.format(' OR '.join(partition_where_clauses))
        return full_partitioning_clause

    def build_timestamp_filter(self):
        return "`timestamp` BETWEEN {0} AND {1}".format(
            int(
                time.mktime(
                    self.start_date.timetuple()
                )
            ),
            int(
                time.mktime(
                    self.end_date.timetuple()
                )
            )
        )
