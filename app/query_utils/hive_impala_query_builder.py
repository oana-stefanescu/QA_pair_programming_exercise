from calendar import monthrange
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from collections import OrderedDict

from app.query_utils.time_range_container import *


def generate_timerange_query(start: datetime, end: datetime, generate_timestamp_clause: bool = True) -> str:
    """
    Generates the timerange query for partitioning that suits both hive and impala queries.

    Args:
        start: The start date in UTC.
        end: The end date in UTC.
        generate_timestamp_clause: If True append a timestamp BETWEEN clause to the query (with the corresponding
            start and end timestamps).

    Raises:
        ValueError: If start date or end date is not in UTC or if start date is after end date.

    Returns:
        The partition query string.
    """
    if start.tzinfo != timezone.utc:
        raise ValueError("Start date has to be in UTC. Instead we have %s" % str(start.tzinfo))
    if end.tzinfo != timezone.utc:
        raise ValueError("End date has to be in UTC. Instead we have %s" % str(end.tzinfo))
    if start > end:
        raise ValueError("Start date has to be before the end date. You have start:%s \tend:%s" % (start, end))

    partition_query_builder = PartitionQueryBuilder(start_date=start, end_date=end)
    partition_filter = partition_query_builder.build_partition_filter()
    if generate_timestamp_clause:
        time_filter = partition_query_builder.build_timestamp_filter()
        return "{0} AND {1}".format(time_filter, partition_filter)
    else:
        return partition_filter


class PartitionQueryBuilder(object):
    """
    Generates the partition string for the given start and end dates.

    Attributes:
        start_date: The start date in UTC.
        end_date: The end date in UTC.
    """

    def __init__(self, start_date: datetime, end_date: datetime):
        """
        Args:
            start_date: The start date in UTC.
            end_date:  The end date in UTC.
        """
        self.start_date = start_date
        self.end_date = end_date

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
        if self.start_date.year == self.end_date.year:
            try:
                months.remove(date.month)
            except ValueError:
                pass

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
        if self.end_date.year == self.start_date.year and self.end_date.month == self.start_date.month:
            try:
                days.remove(date.day)
            except ValueError:
                pass

        return days

    def _get_relevant_hours_of_start_date(self) -> Optional[TimeRangeContainer]:
        """
        Calculates all the relevant hours of the start date that should be scanned. If the start date is the same as the
        end date, it will return `None` and the relevant hours will be calculated by the method
        `_get_relevant_hours_of_end_date`.

        Returns:
            A TimeRangeContainer object holding all the relevant hours of the start date or `None` if the start date is
            the same as the end date.
        """
        if (self.start_date.day == self.end_date.day
           and
           self.start_date.month == self.end_date.month
           and
           self.start_date.year == self.end_date.year):
            return None
        else:
            hours = list(range(self.start_date.hour, 24))
            return TimeRangeContainer(years=[self.start_date.year],
                                      months=[self.start_date.month],
                                      days=[self.start_date.day],
                                      hours=hours)

    def _get_relevant_hours_of_end_date(self) -> TimeRangeContainer:
        """
        Calculates all the relevant hours of the end date that should be scanned.

        Returns:
            A TimeRangeContainer object holding all the relevant hours of the end date that should be scanned.
        """
        if self.start_date.day == self.end_date.day \
                and self.start_date.month == self.end_date.month \
                and self.start_date.year == self.end_date.year:
            first_hour = self.start_date.hour
        else:
            first_hour = 0

        hours = list(range(first_hour, self.end_date.hour + 1))

        return TimeRangeContainer(years=[self.end_date.year],
                                  months=[self.end_date.month],
                                  days=[self.end_date.day],
                                  hours=hours)

    def _get_gap_years(self) -> Optional[TimeRangeContainer]:
        """
        Calculates all the years between the start date year and the end date year that should be scanned completely.

        Returns:
            A TimeRangeContainer object holding the information about the years between the start date and the end date
            that should be scanned completely or `None` if there aren't any.
        """
        if relativedelta(self.end_date, self.start_date).years > 0:
            years = list(range(self.start_date.year + 1, self.end_date.year))
            if years:
                return TimeRangeContainer(years=years)
        return None

    def _get_relevant_days_in_start_date_month(self) -> Optional[TimeRangeContainer]:
        """
        Calculates all the relevant days for the month of the start date. If there are no days in the start date month
        that should be scanned completely, the output will be `None`.

        Returns:
            A TimeRangeContainer object holding the information about all the full days of the start date month or
            `None` if there aren't any.
        """
        last_day_of_month = datetime(year=self.start_date.year, month=self.start_date.month,
                                     day=monthrange(self.start_date.year, self.start_date.month)[1],
                                     hour=23, minute=59, second=59,
                                     tzinfo=timezone.utc)

        if last_day_of_month > self.end_date:
            date_for_diff = self.end_date
        else:
            date_for_diff = last_day_of_month

        if (relativedelta(date_for_diff, self.start_date).days > 0 or
                relativedelta(date_for_diff, self.start_date).hours == 23):
            days = list(range(self.start_date.day + 1, date_for_diff.day + 1))
            days = self._remove_duplicate_day(days, self.end_date)
            if len(days) > 0:
                return TimeRangeContainer(years=[self.start_date.year], months=[self.start_date.month], days=days)
        return None

    def _get_relevant_months_in_start_date_year(self) -> Optional[TimeRangeContainer]:
        """
        Calculates all the relevant months for the year of the start date. If there are no months in the start year that
        should be scanned completely, the output will be `None`.

        Returns:
            A TimeRangeContainer object holding the information about all the full month of the start date year or
            `None` if there aren't any.
        """
        last_day_of_year = datetime(year=self.start_date.year, month=12, day=31, hour=23, minute=59, second=59,
                                    tzinfo=timezone.utc)

        if last_day_of_year > self.end_date:
            date_for_diff = self.end_date
            add_compare_month = False
        else:
            date_for_diff = last_day_of_year
            add_compare_month = True

        if relativedelta(date_for_diff, self.start_date).months > 0:

            compare_month = date_for_diff.month
            months = list(range(self.start_date.month + 1, compare_month))
            # add also the last month if the end date is not in the current year
            if add_compare_month:
                months.append(compare_month)
            months = self._remove_duplicate_month(months, self.start_date)
            if len(months) > 0:
                return TimeRangeContainer(years=[self.start_date.year], months=months)
        return None

    def _get_relevant_days_in_end_date_month(self) -> Optional[TimeRangeContainer]:
        """
        Calculates all the relevant days for the month of the end date. If there are no days in the end date month that
        should be scanned completely, the output will be `None`.

        Returns:
            A TimeRangeContainer object holding the information about all the full days of the end date month or `None`
            if there aren't any.
        """
        first_day_of_end_date_month = datetime(year=self.end_date.year, month=self.end_date.month, day=1,
                                               tzinfo=timezone.utc)

        if self.start_date > first_day_of_end_date_month:
            date_for_diff = self.start_date
        else:
            date_for_diff = first_day_of_end_date_month

        if (relativedelta(self.end_date, date_for_diff).months > 0 or
                relativedelta(self.end_date, date_for_diff).days > 0):
            days = list(range(date_for_diff.day, self.end_date.day))
            days = self._remove_duplicate_day(days, self.start_date)
            if len(days) > 0:
                return TimeRangeContainer(years=[self.end_date.year], months=[self.end_date.month], days=days)
        return None

    def _get_relevant_months_in_end_date_year(self) -> Optional[TimeRangeContainer]:
        """
        Calculates all the relevant months for the year of the end date. If there are no months in the end year that
        should be scanned completely, the output will be `None`.

        Returns:
            A TimeRangeContainer object holding the information about all the full month of the end date year or `None`
            if there aren't any.
        """
        first_day_of_end_date_year = datetime(year=self.end_date.year, month=1, day=1, tzinfo=timezone.utc)

        if self.start_date > first_day_of_end_date_year:
            date_for_diff = self.start_date
        else:
            date_for_diff = first_day_of_end_date_year

        if relativedelta(self.end_date, date_for_diff).months > 0:
            months = list(range(date_for_diff.month, self.end_date.month))
            months = self._remove_duplicate_month(months, self.start_date)
            if len(months) > 0:
                return TimeRangeContainer(years=[self.end_date.year], months=months)
        return None

    @staticmethod
    def _build_filter_for_key(key: str, values: Optional[List[int]] = None) -> Optional[str]:
        """
        Builds a basic filter for a given key and values. The values are assumed to be a list of consecutive
        integers or `None`.

        Args:
            key: The key for which to query.
            values: The values for which to query. List of consecutive integers or `None`.

        Returns:
            The filter clause as a string. It will return `None` if values is `None` or an empty list.
        """
        if not values:
            return None
        if len(values) == 1:
            return "`{0}` = {1}".format(key, str(values[0]))
        else:
            return "`{0}` BETWEEN {1} AND {2}".format(key, values[0], values[-1])

    def _build_partition_filter_for_timerange(self, time_range: Optional[TimeRangeContainer] = None) -> Optional[str]:
        """
        Builds a partition filter for a given TimeRangeContainer object.

        Args:
            time_range: The TimeRangeContainer object.

        Returns:
            The partition filter as a string or `None` if the time_range was `None`.
        """
        if time_range is None:
            return None
        filter_clauses = [
            self._build_filter_for_key(key="year", values=time_range.years),
            self._build_filter_for_key(key="month", values=time_range.months),
            self._build_filter_for_key(key="day", values=time_range.days),
            self._build_filter_for_key(key="hour", values=time_range.hours)
        ]
        # filter out `None` values
        filter_clauses = filter(lambda x: x is not None, filter_clauses)

        return "({0})".format(" AND ".join(filter_clauses))

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
        partition_filters = filter(lambda x: x is not None, partition_filters)

        # remove duplicates but keep order
        d = OrderedDict((e, True) for e in partition_filters)
        partition_filters_deduplicated = d.keys()

        return "({0})".format(" OR ".join(partition_filters_deduplicated))

    def build_timestamp_filter(self) -> str:
        """
        Builds the timestamp filter.

        Returns:
            The timestamp filter clause for the query.
        """
        start_date_timestamp = int(self.start_date.timestamp())
        end_date_timestamp = int(self.end_date.timestamp())

        return "`timestamp` BETWEEN {0} AND {1}".format(start_date_timestamp, end_date_timestamp)
