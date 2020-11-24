from datetime import datetime


class Date(object):
    @staticmethod
    def is_first_date_time_greater(first_date_time: datetime, second_date_time: datetime) -> bool:
        """
        Returns True if the first datetime has a bigger timestamp than the second datetime.

        Args:
            first_date_time: First datetime.
            second_date_time: Second datetime.

        Returns:
            True if the first date is after the second date, False otherwise.
        """
        return first_date_time.timestamp() > second_date_time.timestamp()
