from enum import Enum

DT_LEXICOGRAPHIC_STR_FORMAT = "%Y/%m/%d/%H/%M/%S/%f"


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
