from datetime import datetime, timedelta, timezone
from unittest import mock

import pytest

from news_aggregator_data_access_layer.constants import (
    AGGREGATOR_RUNS_TTL_EXPIRATION_DAYS,
    AggregatorRunStatus,
    ArticleApprovalStatus,
    NewsAggregatorsEnum,
    ResultRefTypes,
)
from news_aggregator_data_access_layer.models.dynamodb import (
    AggregatorRuns,
    NewsAggregators,
    NewsTopics,
    PreviewUsers,
    PublishedArticles,
    SourcedArticles,
    TrustedNewsProviders,
    UntrustedNewsProviders,
    UserTopicSubscriptions,
)

TEST_DT = datetime(2023, 4, 11, 21, 2, 39, 4166)
TEST_DT_END = datetime(2023, 4, 11, 22, 2, 49, 4166)
TEST_DATE_STR = "2022/04/11"


def test_aggregators_init():
    aggregator = NewsAggregators(
        aggregator_id=NewsAggregatorsEnum.BING_NEWS,
        is_active=True,
    )
    assert aggregator.aggregator_id == NewsAggregatorsEnum.BING_NEWS
    assert aggregator.is_active == True


def test_news_topics_init():
    news_topics = NewsTopics(
        topic_id="topic_id",
        topic="topic",
        is_active=True,
        is_published=False,
        date_created=TEST_DT,
        max_aggregator_results=10,
        daily_publishing_limit=5,
        dt_last_aggregated=TEST_DT_END,
        last_publishing_date=TEST_DT_END,
        bing_aggregation_last_end_time=TEST_DT_END,
        news_api_org_aggregation_last_end_time=TEST_DT_END,
        the_news_api_com_aggregation_last_end_time=TEST_DT_END,
    )
    assert news_topics.topic_id == "topic_id"
    assert news_topics.topic == "topic"
    assert news_topics.is_active == True
    assert news_topics.is_published == False
    assert news_topics.date_created == TEST_DT
    assert news_topics.max_aggregator_results == 10
    assert news_topics.daily_publishing_limit == 5
    assert news_topics.dt_last_aggregated == TEST_DT_END
    assert news_topics.last_publishing_date == TEST_DT_END
    assert news_topics.bing_aggregation_last_end_time == TEST_DT_END
    assert news_topics.news_api_org_aggregation_last_end_time == TEST_DT_END
    assert news_topics.the_news_api_com_aggregation_last_end_time == TEST_DT_END


def test_user_topic_subscriptions_init():
    user_topic_subscriptions = UserTopicSubscriptions(
        user_id="user_id",
        topic_id="topic_id",
        date_subscribed=TEST_DT,
    )
    assert user_topic_subscriptions.user_id == "user_id"
    assert user_topic_subscriptions.topic_id == "topic_id"
    assert user_topic_subscriptions.date_subscribed == TEST_DT


def test_trusted_news_providers_init():
    trusted_news_providers = TrustedNewsProviders(
        provider_domain="venturebeat.com",
        provider_name="provider_name",
        trust_score=60,
    )
    assert trusted_news_providers.provider_name == "provider_name"
    assert trusted_news_providers.provider_domain == "venturebeat.com"
    assert trusted_news_providers.trust_score == 60


def test_untrusted_news_providers_init():
    untrusted_news_providers = UntrustedNewsProviders(
        provider_url="venturebeat.com",
    )
    assert untrusted_news_providers.provider_url == "venturebeat.com"


def test_aggregator_runs_init():
    refs = {"type": ResultRefTypes.S3, "bucket": "bucket", "paths": "path1,path2"}
    aggregator_run = AggregatorRuns(
        aggregation_start_date=TEST_DATE_STR,
        aggregation_run_id="aggregation_run_id",
        aggregator_id=NewsAggregatorsEnum.BING_NEWS.value,
        topic_id="topic_id",
        aggregation_data_start_time=TEST_DT,
        aggregation_data_end_time=TEST_DT_END,
        execution_start_time=TEST_DT,
        execution_end_time=TEST_DT_END,
        aggregated_articles_ref=refs,
        aggregated_articles_count=10,
    )
    assert aggregator_run.aggregation_start_date == TEST_DATE_STR
    assert aggregator_run.aggregation_run_id == "aggregation_run_id"
    assert aggregator_run.aggregator_id == NewsAggregatorsEnum.BING_NEWS
    assert aggregator_run.topic_id == "topic_id"
    assert aggregator_run.aggregation_data_start_time == TEST_DT
    assert aggregator_run.aggregation_data_end_time == TEST_DT_END
    assert aggregator_run.execution_start_time == TEST_DT
    assert aggregator_run.execution_end_time == TEST_DT_END
    assert aggregator_run.aggregated_articles_ref.as_dict() == refs
    assert aggregator_run.aggregated_articles_count == 10
    assert aggregator_run.run_status == AggregatorRunStatus.IN_PROGRESS
    assert aggregator_run.expiration - datetime.now(timezone.utc) <= timedelta(
        days=AGGREGATOR_RUNS_TTL_EXPIRATION_DAYS
    )


def test_sourced_articles_init():
    sourced_article = SourcedArticles(
        topic_id="topic_id",
        sourced_article_id="sourced_article_id",
        dt_sourced=TEST_DT,
        dt_published=TEST_DT_END,
        date_published=TEST_DATE_STR,
        title="title",
        topic="topic",
        source_article_categories=["category"],
        source_article_ids=["source_article_ids"],
        source_article_urls=["source_article_urls"],
        providers=["cnn", "fox"],
        short_summary_ref="short_summary_ref",
        medium_summary_ref="medium_summary_ref",
        full_summary_ref="full_summary_ref",
        sourcing_run_id="sourcing run id",
        article_processing_cost=0.1,
    )
    assert sourced_article.topic_id == "topic_id"
    assert sourced_article.sourced_article_id == "sourced_article_id"
    assert sourced_article.dt_sourced == TEST_DT
    assert sourced_article.dt_published == TEST_DT_END
    assert sourced_article.date_published == TEST_DATE_STR
    assert sourced_article.title == "title"
    assert sourced_article.topic == "topic"
    assert sourced_article.source_article_categories == ["category"]
    assert sourced_article.source_article_ids == ["source_article_ids"]
    assert sourced_article.source_article_urls == ["source_article_urls"]
    assert sourced_article.providers == ["cnn", "fox"]
    assert sourced_article.short_summary_ref == "short_summary_ref"
    assert sourced_article.medium_summary_ref == "medium_summary_ref"
    assert sourced_article.full_summary_ref == "full_summary_ref"
    assert sourced_article.thumbs_up == 0
    assert sourced_article.thumbs_down == 0
    assert sourced_article.article_approval_status == ArticleApprovalStatus.PENDING
    assert sourced_article.sourcing_run_id == "sourcing run id"
    assert sourced_article.article_processing_cost == 0.1


def test_published_articles_init():
    published_articles = PublishedArticles(
        topic_id="topic_id",
        publishing_date=TEST_DATE_STR,
        published_article_count=10,
    )
    assert published_articles.topic_id == "topic_id"
    assert published_articles.publishing_date == TEST_DATE_STR
    assert published_articles.published_article_count == 10


def test_preview_users_init():
    preview_users = PreviewUsers(user_id="user_id", name="Peter Jackson")
    assert preview_users.user_id == "user_id"
    assert preview_users.name == "Peter Jackson"
