from enum import Enum

DT_LEXICOGRAPHIC_STR_FORMAT = "%Y/%m/%d/%H/%M/%S/%f"
DT_LEXICOGRAPHIC_STR_REGEX = r"^\d{4}/\d{2}/\d{2}/\d{2}/\d{2}/\d{2}/\d{6}$"
DATE_LEXICOGRAPHIC_STR_FORMAT = "%Y/%m/%d"
DATE_LEXICOGRAPHIC_STR_REGEX = r"^\d{4}/\d{2}/\d{2}$"
RELEVANCE_SORTING_STR = "Relevance"
DATE_SORTING_STR = "Date"
ALL_CATEGORIES_STR = ""


class ResultRefTypes(Enum):
    """
    Enum for result reference types
    """

    S3 = "s3"


class AggregatorRunStatus(Enum):
    """
    Enum for aggregator run status
    """

    COMPLETE = "Complete"
    FAILED = "Failed"
    IN_PROGRESS = "InProgress"


class SummarizationLength(Enum):
    """Enum for summarization length for sourced articles."""

    SHORT = "Short"
    MEDIUM = "Medium"
    LONG = "Long"


class ArticleApprovalStatus(Enum):
    """Enum for article approval status for sourced articles."""

    PENDING = "Pending"
    APPROVED = "Approved"
    REJECTED = "Rejected"
