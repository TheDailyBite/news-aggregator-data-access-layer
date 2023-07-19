from enum import Enum

DT_LEXICOGRAPHIC_STR_FORMAT = "%Y/%m/%d/%H/%M/%S/%f"
DT_LEXICOGRAPHIC_DASH_STR_FORMAT = "%Y-%m-%d-%H-%M-%S-%f"
DT_LEXICOGRAPHIC_STR_REGEX = r"^\d{4}/\d{2}/\d{2}/\d{2}/\d{2}/\d{2}/\d{6}$"
DATE_LEXICOGRAPHIC_STR_FORMAT = "%Y/%m/%d"
DATE_LEXICOGRAPHIC_DASH_STR_FORMAT = "%Y-%m-%d"
DATE_LEXICOGRAPHIC_STR_REGEX = r"^\d{4}/\d{2}/\d{2}$"
RELEVANCE_SORTING_STR = "Relevance"
DATE_SORTING_STR = "Date"
NO_CATEGORY_STR = "n/a"
ARTICLE_NOT_SOURCED_TAGS_FLAG = "False"
ARTICLE_SOURCED_TAGS_FLAG = "True"
DATE_PUBLISHED_ARTICLE_REGEX = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\+00\:00$"
AGGREGATOR_RUNS_TTL_EXPIRATION_DAYS = 90
SUPPORTED_AGGREGATION_CATEGORIES = {
    "business",
    "entertainment",
    "health",
    "politics",
    "products",
    "science-and-technology",
    "sports",
    "us",
    "world",
    "world_africa",
    "world_americas",
    "world_asia",
    "world_europe",
    "world_middleeast",
}


class NewsAggregatorsEnum(str, Enum):
    """
    Enum for news aggregators (only add actually implemented ones)
    """

    BING_NEWS = "bingnews"
    NEWS_API_ORG = "newsapi.org"
    THE_NEWS_API_COM = "thenewsapi.com"

    @classmethod
    def get_member_by_value(cls, value):
        """
        Get enum member by value
        """
        for member in cls:
            if member.value == value:
                return member
        raise ValueError(f"Invalid value {value} for enum {cls}")


class ResultRefTypes(str, Enum):
    """
    Enum for result reference types
    """

    S3 = "s3"


class AggregatorRunStatus(str, Enum):
    """
    Enum for aggregator run status
    """

    COMPLETE = "Complete"
    FAILED = "Failed"
    IN_PROGRESS = "InProgress"


class SummarizationLength(str, Enum):
    """Enum for summarization length for sourced articles."""

    SHORT = "Short"
    MEDIUM = "Medium"
    FULL = "Full"


class ArticleApprovalStatus(str, Enum):
    """Enum for article approval status for sourced articles."""

    PENDING = "Pending"
    APPROVED = "Approved"
    REJECTED = "Rejected"


class ArticleType(str, Enum):
    """Enum for article type for a raw article."""

    NEWS = "news"


class EmbeddingType(str, Enum):
    """Enum for embedding type."""

    TITLE = "title"
    DESCRIPTION = "description"
    CONTENT = "content"
    TITLE_AND_DESCRIPTION = "title_and_description"
    TITLE_AND_CONTENT = "title_and_content"
    DESCRIPTION_AND_CONTENT = "description_and_content"
    TITLE_AND_DESCRIPTION_AND_CONTENT = "title_and_description_and_content"

    @classmethod
    def get_member_by_value(cls, value):
        """
        Get enum member by value
        """
        for member in cls:
            if member.value == value:
                return member
        raise ValueError(f"Invalid value {value} for enum {cls}")
