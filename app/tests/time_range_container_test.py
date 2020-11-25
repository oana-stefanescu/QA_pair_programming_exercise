from app.query_utils.time_range_container import TimeRangeContainer
import pytest
from pydantic.error_wrappers import ValidationError


def test_valid_time_ranges():
    p = TimeRangeContainer(years=[2020], months=[9], days=[30])
    assert p.years[0] == 2020
    assert p.months[0] == 9
    assert p.days[0] == 30
    assert p.hours is None


def test_without_years():
    with pytest.raises(ValidationError, match="A time range container always requires years."):
        TimeRangeContainer(years=[])


def test_days_without_months():
    with pytest.raises(ValidationError, match="Months has to contain values if days are set."):
        TimeRangeContainer(years=[2020], days=[1])


def test_hours_without_days():
    with pytest.raises(ValidationError, match="Days has to contain values if hours are set."):
        TimeRangeContainer(years=[2020], hours=[1])
