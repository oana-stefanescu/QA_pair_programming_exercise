import time
import pytz
from dateutil import parser
from dateutil import tz
import os


def get_timezone():
    return os.environ.get('DEFAULT_TIMEZONE', 'Europe/Berlin')


class Date:
    @staticmethod
    def convert_date_string_to_utc_datetime(datestring):
        """
        Convert a datestring to UTC datetime.datetime object.
        If there is no time information it will be converted to the default_timezone.

        :param: datestring
        :return: datetime in UTC
        """
        parsed_date = parser.parse(datestring)

        if (parsed_date.tzinfo is None):
            parsed_date = pytz.timezone(get_timezone()).localize(parsed_date)

        return parsed_date.astimezone(tz.tzutc())

    @staticmethod
    def get_timestamp(datetime):
        """
        Get the timestamp for a datetime as integer

        :param: datetime
        :return: int
        """
        return int(time.mktime(datetime.timetuple()))

    @staticmethod
    def is_first_date_time_greater(first_date_time, second_date_time):
        """
        Returns true if the first datetime has a bigger timestamp than the second datetime

        :param first_date_time: datetime
        :param second_date_time: datetime

        :return: bool
        """
        return Date.get_timestamp(first_date_time) > Date.get_timestamp(second_date_time)
