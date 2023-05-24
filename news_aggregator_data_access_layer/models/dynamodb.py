import datetime

from pynamodb.attributes import (
    BooleanAttribute,
    MapAttribute,
    NumberAttribute,
    TTLAttribute,
    UnicodeAttribute,
    UnicodeSetAttribute,
    UTCDateTimeAttribute,
    VersionAttribute,
)
from pynamodb.indexes import AllProjection, GlobalSecondaryIndex, LocalSecondaryIndex
from pynamodb.models import Model
from pynamodb_attributes.unicode_enum import UnicodeEnumAttribute

from news_aggregator_data_access_layer.config import DEPLOYMENT_STAGE, DYNAMODB_HOST, REGION_NAME
from news_aggregator_data_access_layer.constants import (
    ALL_CATEGORIES_STR,
    AggregatorRunStatus,
    ArticleApprovalStatus,
    ResultRefTypes,
)
from news_aggregator_data_access_layer.utils.telemetry import setup_logger

logger = setup_logger(__name__)


def create_tables():
    if not AggregatorRuns.exists():
        logger.info("Creating AggregatorRuns table...")
        AggregatorRuns.create_table(wait=True)
    if not UserTopics.exists():
        logger.info("Creating UserTopics table...")
        UserTopics.create_table(wait=True)
    if not TrustedNewsProviders.exists():
        logger.info("Creating TrustedNewsProviders table...")
        TrustedNewsProviders.create_table(wait=True)
    if not SourcedArticles.exists():
        logger.info("Creating SourcedArticles table...")
        SourcedArticles.create_table(wait=True)


class UserTopics(Model):
    """
    A DynamoDB User Topics model.
    """

    class Meta:
        table_name = f"user-topics-{DEPLOYMENT_STAGE}"
        # Specifies the region
        region = REGION_NAME
        # Optional: Specify the hostname only if it needs to be changed from the default AWS setting
        host = DYNAMODB_HOST
        # Specifies the write capacity - unused for on-demand tables
        write_capacity_units = 1
        # Specifies the read capacity - unused for on-demand tables
        read_capacity_units = 1
        billing_mode = "PAY_PER_REQUEST"

    user_id = UnicodeAttribute(hash_key=True)
    topic = UnicodeAttribute(range_key=True)
    # "" category means non categorical topic (e.g. "Generative+AI" across all categories)
    categories = UnicodeSetAttribute(default=ALL_CATEGORIES_STR)  # type: ignore
    is_active = BooleanAttribute()
    date_created = UTCDateTimeAttribute(default_for_new=datetime.datetime.utcnow())
    max_aggregator_results = NumberAttribute(null=True)


class TrustedNewsProviders(Model):
    """
    A DynamoDB Trusted News Providers model.
    """

    class Meta:
        table_name = f"trusted-news-providers-{DEPLOYMENT_STAGE}"
        # Specifies the region
        region = REGION_NAME
        # Optional: Specify the hostname only if it needs to be changed from the default AWS setting
        host = DYNAMODB_HOST
        # Specifies the write capacity - unused for on-demand tables
        write_capacity_units = 1
        # Specifies the read capacity - unused for on-demand tables
        read_capacity_units = 1
        billing_mode = "PAY_PER_REQUEST"

    provider_name = UnicodeAttribute(hash_key=True)
    provider_url = UnicodeAttribute()
    trust_score = NumberAttribute(default_for_new=50)
    provider_aliases = UnicodeSetAttribute(null=True)


class AggregatorRuns(Model):
    """
    A DynamoDB Aggregator Runs model.
    """

    class Meta:
        table_name = f"aggregator-runs-{DEPLOYMENT_STAGE}"
        # Specifies the region
        region = REGION_NAME
        # Optional: Specify the hostname only if it needs to be changed from the default AWS setting
        host = DYNAMODB_HOST
        # Specifies the write capacity - unused for on-demand tables
        write_capacity_units = 1
        # Specifies the read capacity - unused for on-demand tables
        read_capacity_units = 1
        billing_mode = "PAY_PER_REQUEST"

    # this will be a constant in each aggregator (e.g. "bing")
    aggregator_id = UnicodeAttribute(hash_key=True)
    run_datetime = UTCDateTimeAttribute(range_key=True, default_for_new=datetime.datetime.utcnow())
    run_status = UnicodeEnumAttribute(
        AggregatorRunStatus,
        default_for_new=AggregatorRunStatus.IN_PROGRESS,
    )
    run_end_time = UTCDateTimeAttribute(null=True)
    # {"type": "s3", "bucket": "<s3_bucket>", "key": "<prefix>"}
    result_ref = MapAttribute(null=True)  # type: ignore


class SourcedArticlesGSI1(GlobalSecondaryIndex):  # type: ignore
    """
    This class represents a global secondary index which uses the article approval status as the hash key and the
    article_id as the range key. This is mainly used to query for articles by approval status and approve
    pending articles.
    """

    class Meta:
        # All attributes are projected
        projection = AllProjection()

    article_approval_status = UnicodeEnumAttribute(hash_key=True, enum_type=ArticleApprovalStatus)
    article_id = UnicodeAttribute(range_key=True)


class SourcedArticles(Model):
    """
    A DynamoDB Sourced Articles model.
    """

    class Meta:
        table_name = f"sourced-articles-{DEPLOYMENT_STAGE}"
        # Specifies the region
        region = REGION_NAME
        # Optional: Specify the hostname only if it needs to be changed from the default AWS setting
        host = DYNAMODB_HOST
        # Specifies the write capacity - unused for on-demand tables
        write_capacity_units = 1
        # Specifies the read capacity - unused for on-demand tables
        read_capacity_units = 1
        billing_mode = "PAY_PER_REQUEST"

    # a concatenation of topic and requested category
    topic_requested_category = UnicodeAttribute(hash_key=True)
    # TODO - figure out what this is - maybe it can be <published_date_str>_<article_id> where article id is a slice of uuid4 so that it is still sorted?
    article_id = UnicodeAttribute(range_key=True)
    dt_published = UTCDateTimeAttribute(null=False)
    dt_sourced = UTCDateTimeAttribute(null=False, default_for_new=datetime.datetime.utcnow())
    title = UnicodeAttribute(null=False)
    topic = UnicodeAttribute(null=False)
    # NOTE - this is the labeled category, not the requested one
    category = UnicodeAttribute(null=True)
    original_article_id = UnicodeAttribute(null=False)
    providers = UnicodeSetAttribute(null=True)
    article_approval_status = UnicodeEnumAttribute(
        ArticleApprovalStatus,
        default_for_new=ArticleApprovalStatus.PENDING,
    )
    short_summary_ref = UnicodeAttribute(null=True)
    medium_summary_ref = UnicodeAttribute(null=True)
    long_summary_ref = UnicodeAttribute(null=True)
    thumbs_up = NumberAttribute(default_for_new=0)
    thumbs_down = NumberAttribute(default_for_new=0)
    gsi_1 = SourcedArticlesGSI1()
