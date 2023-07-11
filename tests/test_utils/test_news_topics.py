import pytest

from news_aggregator_data_access_layer.constants import NO_CATEGORY_STR
from news_aggregator_data_access_layer.utils.news_topics import AggregatorCategoryMapper

AGGREGATOR_CATEGORIES_MAPPER = {
    "Business": "business",
    "Entertainment": "entertainment",
    "Health": "health",
    "Politics": "politics",
    "Products": "products",
    "ScienceAndTechnology": "science-and-technology",
    "Sports": "sports",
    "US": "us",
    "World": "world",
    "World_Africa": "world_africa",
    "World_Americas": "world_americas",
    "World_Asia": "world_asia",
    "World_Europe": "world_europe",
    "World_MiddleEast": "world_middleeast",
}


@pytest.fixture
def aggregator_category_mapper():
    return AggregatorCategoryMapper(AGGREGATOR_CATEGORIES_MAPPER)


def test_get_category(aggregator_category_mapper):
    """Tests the get_category() method."""

    # Test for a supported category.
    bing_business_category = aggregator_category_mapper.get_category("Business")
    assert bing_business_category == "business"

    # Test for an unsupported category.
    unsupported_category = aggregator_category_mapper.get_category("foobar")
    assert unsupported_category == NO_CATEGORY_STR
