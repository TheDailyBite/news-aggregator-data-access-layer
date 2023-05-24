import re
from datetime import datetime

from news_aggregator_data_access_layer.exceptions import PublishedDateInvalidFormat


def standardize_published_date(dt_str: str, expected_dt_regex: str) -> str:
    """Creates a standardized datetime string in iso8601 format for published date which includes seconds precision.
    The input datetime string may include fractional seconds precision, but the output will not.
    The input datetime string is expected to be in UTC

    Args:
        dt_str (str): The datetime string to standardize. This is expected to be in UTC.
        expected_dt_regex (str): The regex to use to validate the input datetime string

    Raises:
        PublishedDateInvalidFormat: Raised if the input datetime string does not match the expected regex

    Returns:
        str: The standardized datetime string in iso8601 format with seconds precision
    """
    match = re.match(expected_dt_regex, dt_str)
    if match:
        try:
            non_fractional_dt_part = dt_str.split(".")[0]
            iso_format_non_fractional_dt = non_fractional_dt_part + "+00:00"
            standardized_dt = datetime.fromisoformat(iso_format_non_fractional_dt)
            return standardized_dt.isoformat()
        except Exception as e:
            raise PublishedDateInvalidFormat(dt_str, expected_dt_regex)
    else:
        raise PublishedDateInvalidFormat(dt_str, expected_dt_regex)
