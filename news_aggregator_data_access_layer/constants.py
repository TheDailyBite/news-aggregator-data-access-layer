from enum import Enum

DT_LEXICOGRAPHIC_STR_FORMAT = "%Y/%m/%d/%H/%M/%S/%f"
DT_LEXICOGRAPHIC_STR_REGEX = r"^\d{4}/\d{2}/\d{2}/\d{2}/\d{2}/\d{2}/\d{6}$"
DATE_LEXICOGRAPHIC_STR_FORMAT = "%Y/%m/%d"
DATE_LEXICOGRAPHIC_STR_REGEX = r"^\d{4}/\d{2}/\d{2}$"
RELEVANCE_SORTING_STR = "Relevance"
DATE_SORTING_STR = "Date"
ALL_CATEGORIES_STR = ""
SUMMARIZATION_FAILURE_MESSAGE = "Summarization failed."
SUMMARIZATION_TEMPLATE = """You are a world class news reporter, who is known for writing unbiased, informative, and entertaining articles. Your task is to summarize news articles from differing online news sources. 
            You will summarize the article in three different lengths: long, medium, and short.
            A long article is 90% of full length of the original article (or at most 1800 words, whichever is less).
            A medium article is 50% of full length of the original article (or at most 1000 words, whichever is less).
            A short article is 10% of full length of the original article (or at most 200 words, whichever is less).
            You will be provided the query that was used to find the article for additional context.
            If you are unable to access the article or don't feel confident about the result of your summarization you must answer {failure_message}.
            You are given the following article from URL to summarize:
            Article URL: {url}
            Length of summary: {length}
            Query: {query}
            Summary:"""
# NOTE - in the future the template should probably take multiple articles in and aggregate them into a single summary.


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
