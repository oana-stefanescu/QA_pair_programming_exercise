from dateutil import parser, tz
from datetime import datetime
import pytz

from app.query_utils.hive_impala_query_builder import PartitionQueryBuilder


def test_runtime_one_day():
    """
    Test the partitions if the report timerange is one day.
    """
    query_builder = PartitionQueryBuilder(
        datetime(year=2017, month=5, day=13, hour=22, tzinfo=pytz.utc),
        datetime(year=2017, month=5, day=14, hour=21, minute=59, second=59, tzinfo=pytz.utc)
    )

    expected = "(" \
               "(`year` = 2017 AND `month` = 5 AND `day` = 13 AND `hour` BETWEEN 22 AND 23)" \
               " OR " \
               "(`year` = 2017 AND `month` = 5 AND `day` = 14 AND `hour` BETWEEN 0 AND 21)" \
               ")"
    assert query_builder.build_partition_filter() == expected
    assert query_builder.build_timestamp_filter() == "`timestamp` BETWEEN 1494709200 AND 1494795599"
    assert str(query_builder.end_date) == "2017-05-14 21:59:59+00:00"
    assert str(query_builder.start_date) == "2017-05-13 22:00:00+00:00"


def test_runtime_two_days():
    """
    Test the partitions if the report timerange has two days.
    """
    query_builder = PartitionQueryBuilder(
        parser.parse("2017-05-13 00:00:00").astimezone(tz.tzutc()),
        parser.parse("2017-05-14 23:59:59").astimezone(tz.tzutc())
    )
    expected = "(" \
               "(`year` = 2017 AND `month` = 5 AND `day` = 12 AND `hour` BETWEEN 22 AND 23)" \
               " OR " \
               "(`year` = 2017 AND `month` = 5 AND `day` = 13)" \
               " OR " \
               "(`year` = 2017 AND `month` = 5 AND `day` = 14 AND `hour` BETWEEN 0 AND 21)" \
               ")"
    assert query_builder.build_partition_filter() == expected
    assert query_builder.build_timestamp_filter() == "`timestamp` BETWEEN 1494622800 AND 1494795599"
    assert str(query_builder.end_date) == "2017-05-14 21:59:59+00:00"
    assert str(query_builder.start_date) == "2017-05-12 22:00:00+00:00"


def test_runtime_one_month():
    """
    Test the partitions if the start date and end date are at different months.
    """
    query_builder = PartitionQueryBuilder(
        parser.parse("2017-04-13 00:00:00").astimezone(tz.tzutc()),
        parser.parse("2017-05-14 23:59:59").astimezone(tz.tzutc())
    )
    expected = "(" \
               "(`year` = 2017 AND `month` = 4 AND `day` = 12 AND `hour` BETWEEN 22 AND 23)" \
               " OR " \
               "(`year` = 2017 AND `month` = 4 AND `day` BETWEEN 13 AND 30)" \
               " OR " \
               "(`year` = 2017 AND `month` = 5 AND `day` BETWEEN 1 AND 13)" \
               " OR " \
               "(`year` = 2017 AND `month` = 5 AND `day` = 14 AND `hour` BETWEEN 0 AND 21)" \
               ")"
    assert query_builder.build_partition_filter() == expected


def test_runtime_two_months():
    """
    Test the partitions for more than one month.
    """
    query_builder = PartitionQueryBuilder(
        parser.parse("2017-03-13 00:00:00").astimezone(tz.tzutc()),
        parser.parse("2017-05-14 23:59:59").astimezone(tz.tzutc())
    )
    expected = "(" \
               "(`year` = 2017 AND `month` = 3 AND `day` = 12 AND `hour` = 23)" \
               " OR " \
               "(`year` = 2017 AND `month` = 3 AND `day` BETWEEN 13 AND 31)" \
               " OR " \
               "(`year` = 2017 AND `month` = 4)" \
               " OR " \
               "(`year` = 2017 AND `month` = 5 AND `day` BETWEEN 1 AND 13)" \
               " OR " \
               "(`year` = 2017 AND `month` = 5 AND `day` = 14 AND `hour` BETWEEN 0 AND 21)" \
               ")"
    assert query_builder.build_partition_filter() == expected


def test_summer_wintertime():
    """
    Test if the partitions are calculated correctly during summer winter time change.
    """
    query_builder = PartitionQueryBuilder(
        parser.parse("2017-10-29 00:00:00").astimezone(tz.tzutc()),
        parser.parse("2017-10-29 23:59:59").astimezone(tz.tzutc())
    )
    expected = "(" \
               "(`year` = 2017 AND `month` = 10 AND `day` = 28 AND `hour` BETWEEN 22 AND 23)" \
               " OR " \
               "(`year` = 2017 AND `month` = 10 AND `day` = 29 AND `hour` BETWEEN 0 AND 22)" \
               ")"
    assert query_builder.build_partition_filter() == expected


def test_winter_summertime():
    """
    Test if the partitions are calculated correctly during winter summer time change.
    """

    query_builder = PartitionQueryBuilder(
        parser.parse("2017-03-26 00:00:00").astimezone(tz.tzutc()),
        parser.parse("2017-03-26 23:59:59").astimezone(tz.tzutc())
    )
    expected = "(" \
               "(`year` = 2017 AND `month` = 3 AND `day` = 25 AND `hour` = 23)" \
               " OR " \
               "(`year` = 2017 AND `month` = 3 AND `day` = 26 AND `hour` BETWEEN 0 AND 21)" \
               ")"
    assert query_builder.build_partition_filter() == expected


def test_more_than_one_year():
    """
    Test if the partitions are calculated correctly when the time difference is more than two years.
    """
    query_builder = PartitionQueryBuilder(
        parser.parse("2014-01-26 00:00:00").astimezone(tz.tzutc()),
        parser.parse("2017-05-26 23:59:59").astimezone(tz.tzutc())
    )

    expected = "(" \
               "(`year` = 2014 AND `month` = 1 AND `day` = 25 AND `hour` = 23)" \
               " OR " \
               "(`year` = 2014 AND `month` = 1 AND `day` BETWEEN 26 AND 31)" \
               " OR " \
               "(`year` = 2014 AND `month` BETWEEN 2 AND 12)" \
               " OR " \
               "(`year` BETWEEN 2015 AND 2016)" \
               " OR " \
               "(`year` = 2017 AND `month` BETWEEN 1 AND 4)" \
               " OR " \
               "(`year` = 2017 AND `month` = 5 AND `day` BETWEEN 1 AND 25)" \
               " OR " \
               "(`year` = 2017 AND `month` = 5 AND `day` = 26 AND `hour` BETWEEN 0 AND 21)" \
               ")"
    assert query_builder.build_partition_filter() == expected


def test_end_date_january_next_year():
    """
    Test the change of the years. Here the end date wil be in January.
    """

    query_builder = PartitionQueryBuilder(
        parser.parse("2014-01-26 00:00:00").astimezone(tz.tzutc()),
        parser.parse("2015-01-02 23:59:59").astimezone(tz.tzutc())
    )

    expected = "(" \
               "(`year` = 2014 AND `month` = 1 AND `day` = 25 AND `hour` = 23)" \
               " OR " \
               "(`year` = 2014 AND `month` = 1 AND `day` BETWEEN 26 AND 31)" \
               " OR " \
               "(`year` = 2014 AND `month` BETWEEN 2 AND 12)" \
               " OR " \
               "(`year` = 2015 AND `month` = 1 AND `day` = 1)" \
               " OR " \
               "(`year` = 2015 AND `month` = 1 AND `day` = 2 AND `hour` BETWEEN 0 AND 22)" \
               ")"
    assert query_builder.build_partition_filter() == expected
    assert query_builder.build_timestamp_filter() == "`timestamp` BETWEEN 1390687200 AND 1420235999"


def test_end_date_first_january():
    """
    Test the change of the years. Here the end date wil be in 1st January.
    """
    query_builder = PartitionQueryBuilder(
        parser.parse("2014-01-26 00:00:00").astimezone(tz.tzutc()),
        parser.parse("2015-01-01 23:59:59").astimezone(tz.tzutc())
    )

    expected = "(" \
               "(`year` = 2014 AND `month` = 1 AND `day` = 25 AND `hour` = 23)" \
               " OR " \
               "(`year` = 2014 AND `month` = 1 AND `day` BETWEEN 26 AND 31)" \
               " OR " \
               "(`year` = 2014 AND `month` BETWEEN 2 AND 12)" \
               " OR " \
               "(`year` = 2015 AND `month` = 1 AND `day` = 1 AND `hour` BETWEEN 0 AND 22)" \
               ")"
    assert query_builder.build_partition_filter() == expected
    assert query_builder.build_timestamp_filter() == "`timestamp` BETWEEN 1390687200 AND 1420149599"


def test_end_date_january_in_two_years():
    """
    Test the partitions if the start date is January in two years.
    """

    query_builder = PartitionQueryBuilder(
        parser.parse("2014-01-26 00:00:00").astimezone(tz.tzutc()),
        parser.parse("2016-01-01 23:59:59").astimezone(tz.tzutc())
    )
    expected = "(" \
               "(`year` = 2014 AND `month` = 1 AND `day` = 25 AND `hour` = 23)" \
               " OR " \
               "(`year` = 2014 AND `month` = 1 AND `day` BETWEEN 26 AND 31)" \
               " OR " \
               "(`year` = 2014 AND `month` BETWEEN 2 AND 12)" \
               " OR " \
               "(`year` = 2015)" \
               " OR " \
               "(`year` = 2016 AND `month` = 1 AND `day` = 1 AND `hour` BETWEEN 0 AND 22)" \
               ")"
    assert query_builder.build_partition_filter() == expected


def test_one_second_diff():
    """
    Test the runtime when there is one second difference.
    """
    query_builder = PartitionQueryBuilder(
        parser.parse("2017-08-24T09:58:08+0200").astimezone(tz.tzutc()),
        parser.parse("2017-08-24T09:58:09+0200").astimezone(tz.tzutc())
    )
    assert query_builder.build_partition_filter() == "((`year` = 2017 AND `month` = 8 AND `day` = 24 AND `hour` = 7))"


def test_one_second_diff_in_different_hours():
    """
    Test the runtime when there is one second difference.
    """
    query_builder = PartitionQueryBuilder(
        parser.parse("2017-08-24T09:59:59+0200").astimezone(tz.tzutc()),
        parser.parse("2017-08-24T10:00:00+0200").astimezone(tz.tzutc())
    )
    assert query_builder.build_partition_filter() == "((`year` = 2017 AND `month` = 8 AND `day` = 24 AND `hour` BETWEEN 7 AND 8))"


def test_several_years():
    """
    Test the partitions if the start date is January in two years.
    """

    query_builder = PartitionQueryBuilder(
        parser.parse("2014-01-01 00:00:00+00:00").astimezone(tz.tzutc()),
        parser.parse("2020-12-31 23:59:59+00:00").astimezone(tz.tzutc())
    )

    expected = "(" \
               "(`year` = 2014 AND `month` = 1 AND `day` = 1 AND `hour` BETWEEN 0 AND 23)" \
               " OR " \
               "(`year` = 2014 AND `month` = 1 AND `day` BETWEEN 2 AND 31)" \
               " OR " \
               "(`year` = 2014 AND `month` BETWEEN 2 AND 12)" \
               " OR " \
               "(`year` BETWEEN 2015 AND 2019)" \
               " OR " \
               "(`year` = 2020 AND `month` BETWEEN 1 AND 11)" \
               " OR " \
               "(`year` = 2020 AND `month` = 12 AND `day` BETWEEN 1 AND 30)" \
               " OR " \
               "(`year` = 2020 AND `month` = 12 AND `day` = 31 AND `hour` BETWEEN 0 AND 23)" \
               ")"
    assert query_builder.build_partition_filter() == expected
