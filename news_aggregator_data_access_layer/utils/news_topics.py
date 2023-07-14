from news_aggregator_data_access_layer.constants import (
    NO_CATEGORY_STR,
    SUPPORTED_AGGREGATION_CATEGORIES,
)


# Define the aggregator category mapper class.
class AggregatorCategoryMapper:
    def __init__(self, aggregator_category_mapper):
        if not set(aggregator_category_mapper.values()).issubset(SUPPORTED_AGGREGATION_CATEGORIES):
            raise ValueError(
                "The values of the aggregator category mapper must be a subset of the supported categories."
            )
        self.supported_categories = SUPPORTED_AGGREGATION_CATEGORIES
        self.aggregator_category_mapper = aggregator_category_mapper

    def get_category(self, category):
        """Gets the aggregator's category name for the given category.

        Args:
            category: The category name.

        Returns:
            The internal category mapped name, or NO_CATEGORY_STR if no mapping exists for the aggregator.
        """
        return self.aggregator_category_mapper.get(category, NO_CATEGORY_STR)
