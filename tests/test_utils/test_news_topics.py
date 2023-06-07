import pytest

from news_aggregator_data_access_layer.utils.news_topics import AggregatorCategoryMapper

AGGREGATOR_CATEGORIES_MAPPER = {
    "business": "Business",
    "entertainment": "Entertainment",
    "health": "Health",
    "politics": "Politics",
    "products": "Products",
    "science-and-technology": "ScienceAndTechnology",
    "sports": "Sports",
    "us": "US",
    "world": "World",
    "world_africa": "World_Africa",
    "world_americas": "World_Americas",
    "world_asia": "World_Asia",
    "world_europe": "World_Europe",
    "world_middleeast": "World_MiddleEast",
    "": "",
}


@pytest.fixture
def aggregator_category_mapper():
    return AggregatorCategoryMapper(AGGREGATOR_CATEGORIES_MAPPER)


def test_get_category(aggregator_category_mapper):
    """Tests the get_category() method."""

    # Test for a supported category.
    bing_business_category = aggregator_category_mapper.get_category("business")
    assert bing_business_category == "Business"

    # Test for an unsupported category.
    unsupported_category = aggregator_category_mapper.get_category("foobar")
    assert unsupported_category is None
