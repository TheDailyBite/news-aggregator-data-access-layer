import pytest

from news_aggregator_data_access_layer.exceptions import PublishedDateInvalidFormat
from news_aggregator_data_access_layer.utils.datetime import standardize_published_date

BING_NEWS_PUBLISHED_DATE_REGEX = (
    r"^([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{7}Z)$"
)
TEST_BING_DT_STR = "2021-04-11T21:02:39.0004166Z"


def test_standardize_published_date():
    actual_standardized_dt = standardize_published_date(
        TEST_BING_DT_STR, BING_NEWS_PUBLISHED_DATE_REGEX
    )
    assert actual_standardized_dt == "2021-04-11T21:02:39+00:00"


def test_standardize_published_date_raises():
    with pytest.raises(PublishedDateInvalidFormat) as exc_info:
        standardize_published_date("2021-04-11T21:02:39.00166Z", BING_NEWS_PUBLISHED_DATE_REGEX)
