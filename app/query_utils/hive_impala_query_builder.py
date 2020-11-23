import hashlib
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
        self.partitions = Partitions(missing_hours_of_first_day=self._get_missing_hours_of_first_day(),
                                     missing_days_in_start_date_month=self._get_missing_days_in_start_date_month(),
                                     missing_months_in_start_date_year=self.get_missing_months_in_start_date_year(),
                                     missing_years=self._get_years(),
                                     missing_hours_of_last_day=self._get_missing_hours_of_last_day(),
                                     missing_days_in_end_date_month=self._get_missing_days_in_end_date_month(),
                                     missing_months_in_end_date_year=self._get_missing_months_in_end_date_year())

    def _get_date_part_as_int(self, datetime, date_part):
        return int(datetime.strftime(date_part))

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

    def _get_missing_hours_of_first_day(self) -> MissingHoursOnDay:
        """
        Calculates the hours of the first day.

        :return: Missing hours on the first days
        """
        if not (self.start_day == self.end_day
                and
                self.start_month == self.end_month
                and
                self.start_year == self.end_year
        ):
            hours = []
            for missing_hour in range(self.start_hour, 24):
                hours.append(missing_hour)
            return MissingHoursOnDay(year=self.start_year, month=self.start_month, day=self.start_day, hours=hours)

    def _get_missing_hours_of_last_day(self) -> MissingHoursOnDay:
        """
        Calculates the missing hours for the enddate.

        :return: Missing hours on the last day
        """

        if (self.start_day == self.end_day
                and
                self.start_month == self.end_month
                and
                self.start_year == self.end_year
        ):
            first_hour = self.start_hour
        else:
            first_hour = 0

        hours = []
        for missing_hour in range(first_hour, self.end_hour + 1):
            hours.append(missing_hour)

        return MissingHoursOnDay(year=self.end_year, month=self.end_month, day=self.end_day, hours=hours)

    def _get_years(self):
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
                return MissingYears(years=years)

    def _get_missing_days_in_start_date_month(self) -> MissingDaysInMonth:
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
            if (len(days) > 0):
                return MissingDaysInMonth(year=self.start_year, month=self.start_month, days=days)

    def get_missing_months_in_start_date_year(self) -> MissingMonthsInYear:
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

            # @see testMoreThanOneYear
            compare_month = int(date_for_diff.strftime("%m"))
            if (compare_month == 12):
                compare_month = compare_month + 1

            for missing_month in range(self.start_month + 1, compare_month):
                months.append(missing_month)

            months = self._remove_duplicate_month(months, self.start_date)
            if (len(months) > 0):
                return MissingMonthsInYear(year=self.start_year, months=months)

    def _get_missing_days_in_end_date_month(self) -> MissingDaysInMonth:
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

        partition = {}
        if (relativedelta(self.end_date, date_for_diff).months > 0 or
                relativedelta(self.end_date, date_for_diff).days > 0):
            days = []
            for missing_days in range(int(date_for_diff.strftime("%d")), self.end_day):
                days.append(missing_days)

            days = self._remove_duplicated_day(days, self.start_date)
            if (len(days) > 0):
                return MissingDaysInMonth(year=self.end_year, month=self.end_month, days=days)

    def _get_missing_months_in_end_date_year(self) -> MissingMonthsInYear:
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

        partition = {}
        if (relativedelta(self.end_date, date_for_diff).months > 0):
            months = []
            for missing_month in range(
                    self._get_date_part_as_int(date_for_diff, '%m'),
                    self.end_month):
                months.append(missing_month)

            months = self._remove_duplicate_month(months, self.start_date)
            if (len(months) > 0):
                return MissingMonthsInYear(year=self.end_year, months=months)

    def _build_query_from_partition(self, partition):
        """
        Build the sql string for the given partition parameters.

        Example input:
        {'month': [5], 'day': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13], 'year': [2010]}

        Example output:
        (`month` = 5 AND `day` BETWEEN 1 AND 13 AND `year` = 2010)

        :param partition: Dict with partition informartions
        :return: string
        """
        sorted_partition = sorted(
            partition.items(),
            key=lambda pair: [
                self.YEAR,
                self.MONTH,
                self.DAY,
                self.HOUR
            ].index(pair[0])
        )

        sql_parts = []
        for partition_info in sorted_partition:
            key = partition_info[0]
            values = partition_info[1]

            if len(values) == 1:
                sql_parts.append("`{0}` = {1}".format(key, values[0]))
            else:
                sql_parts.append(
                    "`{0}` BETWEEN {1} AND {2}".
                        format(key, min(values), max(values))
                )

        return '({0})'.format(' AND '.join(sql_parts))

    def build_partition_filter(self):
        """
        Generates the partition query for all necessary partitions
        for the given timerange.

        :return: string
        """
        sorted_partitions = sorted(
            partitions.items(),
            key=lambda pair: [
                self.FIRST_DAY,
                self.MISSING_DAYS_IN_START_DATE_MONTH,
                self.MISSING_MONTHS_IN_START_DATE_YEAR,
                self.MISSING_YEARS,
                self.MISSING_MONTHS_IN_END_DATE_YEAR,
                self.MISSING_DAYS_IN_END_DATE_MONTH,
                self.LAST_DAY
            ].index(pair[0])
        )

        final_partitions = []
        partitionMd5 = {}
        for sorted_partition in sorted_partitions:
            partition_info = sorted_partition[1]
            if (len(partition_info) == 0):
                continue
            partitionQuery = self._build_query_from_partition(partition_info)
            md5 = hashlib.md5(partitionQuery.encode("utf")).hexdigest()
            if md5 not in partitionMd5:
                final_partitions.append(partitionQuery)
                partitionMd5[md5] = True

        return '({0})'.format(' OR '.join(final_partitions))

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
