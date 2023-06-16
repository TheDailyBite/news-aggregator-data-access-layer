import datetime
import uuid

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
    AGGREGATOR_RUNS_TTL_EXPIRATION_DAYS,
    ALL_CATEGORIES_STR,
    AggregatorRunStatus,
    ArticleApprovalStatus,
    NewsAggregatorsEnum,
    ResultRefTypes,
)
from news_aggregator_data_access_layer.utils.telemetry import setup_logger

logger = setup_logger(__name__)


def create_tables():
    if not NewsAggregators.exists():
        logger.info("Creating NewsAggregators table...")
        NewsAggregators.create_table(wait=True)
    if not NewsTopics.exists():
        logger.info("Creating NewsTopics table...")
        NewsTopics.create_table(wait=True)
    if not UserTopicSubscriptions.exists():
        logger.info("Creating UserTopicSubscriptions table...")
        UserTopicSubscriptions.create_table(wait=True)
    if not TrustedNewsProviders.exists():
        logger.info("Creating TrustedNewsProviders table...")
        TrustedNewsProviders.create_table(wait=True)
    if not AggregatorRuns.exists():
        logger.info("Creating AggregatorRuns table...")
        AggregatorRuns.create_table(wait=True)
    if not SourcedArticles.exists():
        logger.info("Creating SourcedArticles table...")
        SourcedArticles.create_table(wait=True)
    if not PublishedArticles.exists():
        logger.info("Creating PublishedArticles table...")
        PublishedArticles.create_table(wait=True)


def get_uuid4_attribute() -> str:
    return str(uuid.uuid4())


def get_current_dt_utc_attribute() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


class NewsAggregators(Model):
    """
    A DynamoDB NewsAggregators model.
    """

    class Meta:
        table_name = f"news-aggregators-{DEPLOYMENT_STAGE}"
        # Specifies the region
        region = REGION_NAME
        # Optional: Specify the hostname only if it needs to be changed from the default AWS setting
        host = DYNAMODB_HOST
        # Specifies the write capacity - unused for on-demand tables
        write_capacity_units = 1
        # Specifies the read capacity - unused for on-demand tables
        read_capacity_units = 1
        billing_mode = "PAY_PER_REQUEST"

    aggregator_id = UnicodeEnumAttribute(NewsAggregatorsEnum, hash_key=True)
    is_active = BooleanAttribute()


class NewsTopics(Model):
    """
    A DynamoDB News Topics model.
    """

    class Meta:
        table_name = f"news-topics-{DEPLOYMENT_STAGE}"
        # Specifies the region
        region = REGION_NAME
        # Optional: Specify the hostname only if it needs to be changed from the default AWS setting
        host = DYNAMODB_HOST
        # Specifies the write capacity - unused for on-demand tables
        write_capacity_units = 1
        # Specifies the read capacity - unused for on-demand tables
        read_capacity_units = 1
        billing_mode = "PAY_PER_REQUEST"

    topic_id = UnicodeAttribute(hash_key=True)
    # NOTE - maybe a GSI can be created for topic + category in the future to avoid a scan
    topic = UnicodeAttribute()
    category = UnicodeAttribute()
    # this controls if aggregation and sourcing occur for this topic
    is_active = BooleanAttribute()
    # this controls if the topic is shown to users
    is_published = BooleanAttribute()
    date_created = UTCDateTimeAttribute()
    max_aggregator_results = NumberAttribute()
    daily_publishing_limit = NumberAttribute()
    dt_last_aggregated = UTCDateTimeAttribute(null=True)
    last_publishing_date = UTCDateTimeAttribute(null=True)
    bing_aggregation_last_end_time = UTCDateTimeAttribute(null=True)
    news_api_org_aggregation_last_end_time = UTCDateTimeAttribute(null=True)
    # NOTE - add other aggregator attributes here
    version = VersionAttribute()


class UserTopicSubscriptionsGSI1(GlobalSecondaryIndex):  # type: ignore
    """
    This class represents a global secondary index which uses the topic_id as the hash key and the
    user_id as the range key. This is mainly used to query for users subscribed to a topic id when the topic id is deleted
    (which shouldn't really happen but I guess it could if it produces bad results).
    """

    class Meta:
        # All attributes are projected
        projection = AllProjection()

    topic_id = UnicodeAttribute(hash_key=True)
    user_id = UnicodeAttribute(range_key=True)


class UserTopicSubscriptions(Model):
    """
    A DynamoDB User Topic Subscriptions model.
    """

    class Meta:
        table_name = f"user-topic-subscriptions-{DEPLOYMENT_STAGE}"
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
    topic_id = UnicodeAttribute(range_key=True)
    date_subscribed = UTCDateTimeAttribute()
    gsi_1 = UserTopicSubscriptionsGSI1()


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

    provider_domain = UnicodeAttribute(hash_key=True)
    provider_name = UnicodeAttribute()
    trust_score = NumberAttribute(default_for_new=50)


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

    # this will be a date when the aggregator started, without time
    aggregation_start_date = UnicodeAttribute(hash_key=True)
    aggregation_run_id = UnicodeAttribute(range_key=True)
    aggregator_id = UnicodeEnumAttribute(NewsAggregatorsEnum)
    topic_id = UnicodeAttribute()
    aggregation_data_start_time = UTCDateTimeAttribute()
    aggregation_data_end_time = UTCDateTimeAttribute()
    run_status = UnicodeEnumAttribute(
        AggregatorRunStatus,
        default_for_new=AggregatorRunStatus.IN_PROGRESS,
    )
    execution_start_time = UTCDateTimeAttribute()
    execution_end_time = UTCDateTimeAttribute(null=True)
    # {"type": "s3", "bucket": "<s3_bucket>", "paths": "<comme-separated-prefixes>"}
    aggregated_articles_ref = MapAttribute(null=True)  # type: ignore
    aggregated_articles_count = NumberAttribute(default_for_new=0)
    expiration = TTLAttribute(
        default_for_new=datetime.timedelta(days=AGGREGATOR_RUNS_TTL_EXPIRATION_DAYS)
    )


class SourcedArticlesGSI1(GlobalSecondaryIndex):  # type: ignore
    """
    This class represents a global secondary index which uses the article approval status as the hash key and the
    sourced_article_id as the range key. This is mainly used to query for articles by approval status and approve
    pending articles.
    """

    class Meta:
        # All attributes are projected
        projection = AllProjection()

    article_approval_status = UnicodeEnumAttribute(hash_key=True, enum_type=ArticleApprovalStatus)
    sourced_article_id = UnicodeAttribute(range_key=True)


class SourcedArticlesLSI1(LocalSecondaryIndex):  # type: ignore
    """
    This class represents a local secondary index which uses the topic_id as the hash key and the
    date_published as the range key. This is mainly used to query for articles by topic id and date published.
    """

    class Meta:
        # All attributes are projected
        projection = AllProjection()

    topic_id = UnicodeAttribute(hash_key=True)
    date_published = UnicodeAttribute(range_key=True)


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

    topic_id = UnicodeAttribute(hash_key=True)
    # TODO - figure out what this is - maybe it can be <published_date_str>_<article_id> where article id is a slice of uuid4 so that it is still sorted?
    sourced_article_id = UnicodeAttribute(range_key=True)
    dt_sourced = UTCDateTimeAttribute()
    dt_published = UTCDateTimeAttribute()
    date_published = UnicodeAttribute()
    title = UnicodeAttribute()
    topic = UnicodeAttribute()
    # NOTE - this is the labeled category, not the requested one
    labeled_category = UnicodeAttribute(null=True)
    source_article_ids = UnicodeSetAttribute()
    source_article_urls = UnicodeSetAttribute()
    providers = UnicodeSetAttribute()
    article_approval_status = UnicodeEnumAttribute(
        ArticleApprovalStatus,
        default_for_new=ArticleApprovalStatus.PENDING,
    )
    short_summary_ref = UnicodeAttribute()
    medium_summary_ref = UnicodeAttribute()
    full_summary_ref = UnicodeAttribute()
    thumbs_up = NumberAttribute(default_for_new=0)
    thumbs_down = NumberAttribute(default_for_new=0)
    sourcing_run_id = UnicodeAttribute()
    gsi_1 = SourcedArticlesGSI1()
    lsi_1 = SourcedArticlesLSI1()


class PublishedArticles(Model):
    """
    A DynamoDB Published Articles model.
    """

    class Meta:
        table_name = f"published-articles-{DEPLOYMENT_STAGE}"
        # Specifies the region
        region = REGION_NAME
        # Optional: Specify the hostname only if it needs to be changed from the default AWS setting
        host = DYNAMODB_HOST
        # Specifies the write capacity - unused for on-demand tables
        write_capacity_units = 1
        # Specifies the read capacity - unused for on-demand tables
        read_capacity_units = 1
        billing_mode = "PAY_PER_REQUEST"

    topic_id = UnicodeAttribute(hash_key=True)
    publishing_date = UnicodeAttribute(range_key=True)
    published_article_count = NumberAttribute(default_for_new=0)
    version = VersionAttribute()
