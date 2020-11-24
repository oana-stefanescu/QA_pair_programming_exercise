from dateutil import parser, tz
from datetime import datetime
import pytz
import pytest

from app.query_utils.hive_impala_query_builder import PartitionQueryBuilder, generate_timerange_query


def test_generate_timerange_query_errors():
    """
    Checks that the correct assertions are thrown
    """
    start_no_time_zone = datetime(year=2017, month=5, day=13, hour=22)
    end_no_time_zone = datetime(year=2019, month=5, day=14, hour=22)
    start_correct_time_zone = datetime(year=2017, month=5, day=13, hour=22, tzinfo=pytz.utc)
    end_wrong_time_zone = datetime(year=2017, month=5, day=14, hour=22, tzinfo=pytz.timezone("US/Eastern"))
    with pytest.raises(ValueError, match="Start date has to be in UTC"):
        generate_timerange_query(start_no_time_zone, end_no_time_zone)
    with pytest.raises(ValueError, match="End date has to be in UTC"):
        generate_timerange_query(start_correct_time_zone, end_wrong_time_zone)
    with pytest.raises(ValueError, match="Start date has to be before the end date"):
        generate_timerange_query(pytz.utc.localize(end_no_time_zone), start_correct_time_zone)


def test_generate_timerange_query():
    """
    Test if both partitions and timestamps are generated correctly
    """
    start_time = datetime(year=2017, month=5, day=13, hour=15, tzinfo=pytz.utc)
    end_time = datetime(year=2019, month=7, day=8, hour=12, tzinfo=pytz.utc)

    complete_time_filter = generate_timerange_query(start_time, end_time)
    expected = "`timestamp` BETWEEN 1494687600 AND 1562587200" \
               " AND " \
               "(" \
               "(`year` = 2017 AND `month` = 5 AND `day` = 13 AND `hour` BETWEEN 15 AND 23)" \
               " OR " \
               "(`year` = 2017 AND `month` = 5 AND `day` BETWEEN 14 AND 31)" \
               " OR " \
               "(`year` = 2017 AND `month` BETWEEN 6 AND 12)" \
               " OR " \
               "(`year` = 2018)" \
               " OR " \
               "(`year` = 2019 AND `month` BETWEEN 1 AND 6)" \
               " OR " \
               "(`year` = 2019 AND `month` = 7 AND `day` BETWEEN 1 AND 7)" \
               " OR " \
               "(`year` = 2019 AND `month` = 7 AND `day` = 8 AND `hour` BETWEEN 0 AND 12)" \
               ")"
    assert complete_time_filter == expected

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
    assert query_builder.build_timestamp_filter() == "`timestamp` BETWEEN 1494712800 AND 1494799199"
    assert str(query_builder.end_date) == "2017-05-14 21:59:59+00:00"
    assert str(query_builder.start_date) == "2017-05-13 22:00:00+00:00"


def test_runtime_two_days():
    """
    Test the partitions if the report timerange has two days.
    """
    query_builder = PartitionQueryBuilder(
        datetime(year=2017, month=5, day=12, hour=22, tzinfo=pytz.utc),
        datetime(year=2017, month=5, day=14, hour=21, minute=59, second=59, tzinfo=pytz.utc)
    )
    expected = "(" \
               "(`year` = 2017 AND `month` = 5 AND `day` = 12 AND `hour` BETWEEN 22 AND 23)" \
               " OR " \
               "(`year` = 2017 AND `month` = 5 AND `day` = 13)" \
               " OR " \
               "(`year` = 2017 AND `month` = 5 AND `day` = 14 AND `hour` BETWEEN 0 AND 21)" \
               ")"
    assert query_builder.build_partition_filter() == expected
    assert query_builder.build_timestamp_filter() == "`timestamp` BETWEEN 1494626400 AND 1494799199"
    assert str(query_builder.end_date) == "2017-05-14 21:59:59+00:00"
    assert str(query_builder.start_date) == "2017-05-12 22:00:00+00:00"


def test_runtime_one_month():
    """
    Test the partitions if the start date and end date are at different months.
    """
    query_builder = PartitionQueryBuilder(
        datetime(year=2017, month=4, day=12, hour=22, tzinfo=pytz.utc),
        datetime(year=2017, month=5, day=14, hour=23,minute=59, second=59, tzinfo=pytz.utc)
    )
    expected = "(" \
               "(`year` = 2017 AND `month` = 4 AND `day` = 12 AND `hour` BETWEEN 22 AND 23)" \
               " OR " \
               "(`year` = 2017 AND `month` = 4 AND `day` BETWEEN 13 AND 30)" \
               " OR " \
               "(`year` = 2017 AND `month` = 5 AND `day` BETWEEN 1 AND 13)" \
               " OR " \
               "(`year` = 2017 AND `month` = 5 AND `day` = 14 AND `hour` BETWEEN 0 AND 23)" \
               ")"
    assert query_builder.build_partition_filter() == expected


def test_runtime_two_months():
    """
    Test the partitions for more than one month.
    """
    query_builder = PartitionQueryBuilder(
        datetime(year=2017, month=3, day=12, hour=23, tzinfo=pytz.utc),
        datetime(year=2017, month=5, day=14, hour=23, minute=59, second=59, tzinfo=pytz.utc)
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
               "(`year` = 2017 AND `month` = 5 AND `day` = 14 AND `hour` BETWEEN 0 AND 23)" \
               ")"
    assert query_builder.build_partition_filter() == expected


def test_more_than_one_year():
    """
    Test if the partitions are calculated correctly when the time difference is more than two years.
    """
    query_builder = PartitionQueryBuilder(
        datetime(year=2014, month=1, day=25, hour=23, tzinfo=pytz.utc),
        datetime(year=2017, month=5, day=26, hour=21, minute=59, second=59, tzinfo=pytz.utc)
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
        datetime(year=2014, month=1, day=25, hour=23, tzinfo=pytz.utc),
        datetime(year=2015, month=1, day=2, hour=22, minute=59, second=59, tzinfo=pytz.utc)
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
    assert query_builder.build_timestamp_filter() == "`timestamp` BETWEEN 1390690800 AND 1420239599"


def test_end_date_first_january():
    """
    Test the change of the years. Here the end date wil be in 1st January.
    """
    query_builder = PartitionQueryBuilder(
        datetime(year=2014, month=1, day=25, hour=23, tzinfo=pytz.utc),
        datetime(year=2015, month=1, day=1, hour=22, minute=59, second=59, tzinfo=pytz.utc)
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
    assert query_builder.build_timestamp_filter() == "`timestamp` BETWEEN 1390690800 AND 1420153199"


def test_end_date_january_in_two_years():
    """
    Test the partitions if the start date is January in two years.
    """

    query_builder = PartitionQueryBuilder(
        datetime(year=2014, month=1, day=25, hour=23, tzinfo=pytz.utc),
        datetime(year=2016, month=1, day=1, hour=22, minute=59, second=59, tzinfo=pytz.utc)
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
        datetime(year=2017, month=8, day=24, hour=7, minute=58, second=8, tzinfo=pytz.utc),
        datetime(year=2017, month=8, day=24, hour=7, minute=58, second=9, tzinfo=pytz.utc)
    )

    expected = "((`year` = 2017 AND `month` = 8 AND `day` = 24 AND `hour` = 7))"

    assert query_builder.build_partition_filter() == expected


def test_one_second_diff_in_different_hours():
    """
    Test the runtime when there is one second difference.
    """
    query_builder = PartitionQueryBuilder(
        datetime(year=2017, month=8, day=24, hour=7, minute=59, second=59, tzinfo=pytz.utc),
        datetime(year=2017, month=8, day=24, hour=8, tzinfo=pytz.utc)
    )

    expected = "((`year` = 2017 AND `month` = 8 AND `day` = 24 AND `hour` BETWEEN 7 AND 8))"

    assert query_builder.build_partition_filter() == expected


def test_several_years():
    """
    Test the partitions if the start date is January in two years.
    """

    query_builder = PartitionQueryBuilder(
        datetime(year=2014, month=1, day=1, hour=0, tzinfo=pytz.utc),
        datetime(year=2020, month=12, day=31, hour=23, minute=59, second=59, tzinfo=pytz.utc)
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
