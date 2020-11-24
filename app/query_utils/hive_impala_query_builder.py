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
    Generates the timerange query for partitioning that suits both hive and impala queries.

    Args:
        start: The start date in UTC.
        end: The end date in UTC.

    Returns:
        The partition query string.
    """
    assert start.tzinfo == pytz.utc and end.tzinfo == pytz.utc
    assert Date.is_first_date_time_greater(start, end)

    partition_query_builder = PartitionQueryBuilder(start_date=start, end_date=end)
    partition_filter = partition_query_builder.build_partition_filter()
    time_filter = partition_query_builder.build_timestamp_filter()
    complete_filter = "{0} AND {1}".format(time_filter, partition_filter)

    return complete_filter


class PartitionQueryBuilder:
    """
    Generates the partition string for the given start and end dates.

    Attributes:
        start_date: The start date in UTC.
        end_date: The end date in UTC.
        start_hour: The hour of the start date.
        start_day: The day of the start date.
        start_month: The month of the start date.
        start_year: The year of the start date.
        end_hour: The hour of the end date.
        end_day: The day of the end date.
        end_month: The month of the end date.
        end_year: The year of the end date.
    """
    def __init__(self, start_date: datetime, end_date: datetime):
        """
        Args:
            start_date: The start date in UTC.
            end_date:  The end date in UTC.
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
                                     relevant_years=self._get_gap_years(),
                                     relevant_hours_of_end_date=self._get_relevant_hours_of_end_date(),
                                     relevant_days_in_end_date_month=self._get_relevant_days_in_end_date_month(),
                                     relevant_months_in_end_date_year=self._get_relevant_months_in_end_date_year())

    @staticmethod
    def _get_date_part_as_int(date_time: datetime, date_part: str) -> int:
        return int(date_time.strftime(date_part))

    def _get_start_hour(self) -> int:
        return self._get_date_part_as_int(self.start_date, '%H')

    def _get_start_day(self) -> int:
        return self._get_date_part_as_int(self.start_date, '%d')

    def _get_start_month(self) -> int:
        return self._get_date_part_as_int(self.start_date, '%m')

    def _get_start_year(self) -> int:
        return self._get_date_part_as_int(self.start_date, '%Y')

    def _get_end_hour(self) -> int:
        return self._get_date_part_as_int(self.end_date, '%H')

    def _get_end_day(self) -> int:
        return self._get_date_part_as_int(self.end_date, '%d')

    def _get_end_month(self) -> int:
        return self._get_date_part_as_int(self.end_date, '%m')

    def _get_end_year(self) -> int:
        return self._get_date_part_as_int(self.end_date, '%Y')

    def _remove_duplicate_month(self, months: List[int], date: datetime) -> List[int]:
        """
        If the year of the start date is the same as the year of the end date, the methods
        `_get_relevant_months_in_start_date_year` and `_get_relevant_months_in_end_date_year`
        create more partitions than are actually needed.
        The duplicate month is removed here.

        Args:
            months: List of months as integers.
            date: The calculated date for the difference.

        Returns:
            List of months as integers.
        """
        if self.start_year == self.end_year:
            month = self._get_date_part_as_int(date, '%m')
            if month in months:
                months.remove(month)

        return months

    def _remove_duplicate_day(self, days: List[int], date: datetime) -> List[int]:
        """
        If the year and the month of the start date are the same as the year and the month
        of the end date, the methods `_get_relevant_days_in_start_date_month` and
        `_get_relevant_days_in_end_date_month` create more partitions than are actually needed.
        The duplicate day is removed here.

        Args:
            days: List of days as integers.
            date: The calculated date for the difference.

        Returns:
            List of days as integers.
        """
        if (self.end_year == self.start_year
                and
                self.end_month == self.start_month):
            day = self._get_date_part_as_int(date, '%d')
            if day in days:
                days.remove(day)

        return days

    def _get_relevant_hours_of_start_date(self) -> Optional[TimeRange]:
        """
        Calculates all the relevant hours of the start date. If the start date is the same as the end date, it will
        return `None` and the relevant hours will be calculated by the method `_get_relevant_hours_of_end_date`.

        Returns:
            A TimeRange object holding all the relevant hours of the start date or `None` if the start date is the same
            as the end date.
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
        Calculates all the relevant hours of the end date.

        Returns:
            A TimeRange object holding all the relevant hours of the end date.
        """
        if self.start_day == self.end_day and self.start_month == self.end_month and self.start_year == self.end_year:
            first_hour = self.start_hour
        else:
            first_hour = 0

        hours = []
        for hour in range(first_hour, self.end_hour + 1):
            hours.append(hour)

        return TimeRange(years=[self.end_year], months=[self.end_month], days=[self.end_day], hours=hours)

    def _get_gap_years(self) -> Optional[TimeRange]:
        """
        Calculates all the missing years between the start date and the end date if there are any.

        Returns:
            A TimeRange object holding the information about the gap years between the start date and the end date or
            `None` if there aren't any.
        """
        if relativedelta(self.end_date, self.start_date).years > 0:
            years = []
            for missing_year in range(self.start_year + 1, self.end_year):
                years.append(missing_year)
            if len(years) > 0:
                return TimeRange(years=years)

    def _get_relevant_days_in_start_date_month(self) -> Optional[TimeRange]:
        """
        Calculates all the relevant days for the month of the start date.

        Returns:
            A TimeRange object holding the information about the days of the start date month.
            TODO: when None?
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
            for day in range(
                    self.start_day + 1,
                    self._get_date_part_as_int(date_for_diff, "%d") + 1
            ):
                days.append(day)

            days = self._remove_duplicate_day(days, self.end_date)
            if len(days) > 0:
                return TimeRange(years=[self.start_year], months=[self.start_month], days=days)

    def _get_relevant_months_in_start_date_year(self) -> Optional[TimeRange]:
        """
        Calculates all the relevant months for the year of the start date.

        Returns:
            A TimeRange object holding the information about the months of the start date year.
            TODO: when None?
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

            for month in range(self.start_month + 1, compare_month):
                months.append(month)

            months = self._remove_duplicate_month(months, self.start_date)
            if len(months) > 0:
                return TimeRange(years=[self.start_year], months=months)

    def _get_relevant_days_in_end_date_month(self) -> Optional[TimeRange]:
        """
        Calculates all the relevant days for the month of the end date.

        Returns:
            A TimeRange object holding the information about the days of the end date month.
            TODO: when None?
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

            days = self._remove_duplicate_day(days, self.start_date)
            if len(days) > 0:
                return TimeRange(years=[self.end_year], months=[self.end_month], days=days)

    def _get_relevant_months_in_end_date_year(self) -> Optional[TimeRange]:
        """
        Calculates all the relevant months for the year of the end date.

        Returns:
            A TimeRange object holding the information about the months of the end date year.
            TODO: when None?
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
    def _build_filter_for_key(key: str, values: Optional[List[int]] = None) -> Optional[str]:
        """
        Builds a basic filter for a given key and values. The values are assumed to be a list of consecutive
        integers without any gaps or `None`.

        Args:
            key: The key for which to query.
            values: The values for which to query. List of consecutive integers or `None`.

        Returns:
            The filter clause as a string. It will return `None` if the values are `None` or an empty list.
        """
        if not values:
            return None
        if len(values) == 1:
            return "`{0}` = {1}".format(key, str(values[0]))
        else:
            return "`{0}` BETWEEN {1} AND {2}".format(key, str(min(values)), str(max(values)))

    def _build_partition_filter_for_timerange(self, time_range: Optional[TimeRange] = None) -> Optional[str]:
        """
        Builds a partition filter for a given TimeRange object.

        Args:
            time_range: The TimeRange object.

        Returns:
            The partition filter as a string or `None` if the time_range was `None`.

        """
        if time_range is None:
            return None
        filter_clauses = [
            self._build_filter_for_key('year', time_range.years),
            self._build_filter_for_key('month', time_range.months),
            self._build_filter_for_key('day', time_range.days),
            self._build_filter_for_key('hour', time_range.hours)
        ]
        # filter out `None` values
        filter_clauses = list(filter(lambda x: x is not None, filter_clauses))
        # combine to full filter clause
        partition_filter = '({0})'.format(' AND '.join(filter_clauses))
        return partition_filter

    def build_partition_filter(self) -> str:
        """
        Builds the complete partition filter.

        Returns:
            The complete partition filter clause for the query.

        """
        partition_filters = [
            self._build_partition_filter_for_timerange(self._get_relevant_hours_of_start_date()),
            self._build_partition_filter_for_timerange(self._get_relevant_days_in_start_date_month()),
            self._build_partition_filter_for_timerange(self._get_relevant_months_in_start_date_year()),
            self._build_partition_filter_for_timerange(self._get_gap_years()),
            self._build_partition_filter_for_timerange(self._get_relevant_months_in_end_date_year()),
            self._build_partition_filter_for_timerange(self._get_relevant_days_in_end_date_month()),
            self._build_partition_filter_for_timerange(self._get_relevant_hours_of_end_date())
        ]
        # filter out `None` values
        partition_filters = list(filter(lambda x: x is not None, partition_filters))
        # remove duplicates
        # TODO: how can this happen?
        partition_filters = list(set(partition_filters))
        # combine to full partitioning clause
        full_partition_filter = '({0})'.format(' OR '.join(partition_filters))
        return full_partition_filter

    def build_timestamp_filter(self) -> str:
        """
        Builds the timestamp filter.

        Returns:
            The timestamp filter clause for the query.

        """
        timestamp_start_date = int(time.mktime(self.start_date.timetuple()))
        timestamp_end_date = int(time.mktime(self.end_date.timetuple()))
        timestamp_filter = "`timestamp` BETWEEN {0} AND {1}".format(timestamp_start_date, timestamp_end_date)

        return timestamp_filter
