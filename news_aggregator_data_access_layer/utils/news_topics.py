from news_aggregator_data_access_layer.constants import SUPPORTED_AGGREGATION_CATEGORIES


# Define the aggregator category mapper class.
class AggregatorCategoryMapper:
    def __init__(self, aggregator_category_mapper):
        if set(aggregator_category_mapper.keys()) != SUPPORTED_AGGREGATION_CATEGORIES:
            raise ValueError(
                "The keys of the aggregator category mapper must match the supported categories."
            )
        self.supported_categories = SUPPORTED_AGGREGATION_CATEGORIES
        self.aggregator_category_mapper = aggregator_category_mapper

    def get_category(self, category):
        """Gets the aggregator's category nane for the given category.

        Args:
            category: The category name.

        Returns:
            The category name, or None if the category is not supported by the aggregator.
        """
        return self.aggregator_category_mapper.get(category)
