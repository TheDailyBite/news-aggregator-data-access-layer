from datetime import datetime
from unittest import mock

import pytest

from news_aggregator_data_access_layer.constants import (
    AGGREGATION_NOT_SOURCED_IDENTIFIER,
    AggregatorRunStatus,
    ArticleApprovalStatus,
    ResultRefTypes,
)
from news_aggregator_data_access_layer.models.dynamodb import (
    AggregatorRuns,
    NewsTopics,
    SourcedArticles,
    TrustedNewsProviders,
    UserTopicSubscriptions,
)

TEST_DT = datetime(2023, 4, 11, 21, 2, 39, 4166)
TEST_DT_END = datetime(2023, 4, 11, 22, 2, 49, 4166)
TEST_DATE_STR = "2022/04/11"


def test_news_topics_init():
    news_topics = NewsTopics(
        topic_id="topic_id",
        topic="topic",
        category="category",
        is_active=True,
        date_created=TEST_DT,
        max_aggregator_results=10,
        dt_last_aggregated=TEST_DT_END,
        bing_aggregation_last_end_time=TEST_DT_END,
    )
    assert news_topics.topic_id == "topic_id"
    assert news_topics.topic == "topic"
    assert news_topics.category == "category"
    assert news_topics.is_active == True
    assert news_topics.date_created == TEST_DT
    assert news_topics.max_aggregator_results == 10
    assert news_topics.dt_last_aggregated == TEST_DT_END
    assert news_topics.bing_aggregation_last_end_time == TEST_DT_END


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


def test_aggregator_runs_init():
    refs = {"type": ResultRefTypes.S3, "bucket": "bucket", "key": "key"}
    aggregator_run = AggregatorRuns(
        aggregation_start_date=TEST_DATE_STR,
        aggregation_run_id="aggregation_run_id",
        aggregator_id="aggregator_id",
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
    assert aggregator_run.aggregator_id == "aggregator_id"
    assert aggregator_run.topic_id == "topic_id"
    assert aggregator_run.aggregation_data_start_time == TEST_DT
    assert aggregator_run.aggregation_data_end_time == TEST_DT_END
    assert aggregator_run.execution_start_time == TEST_DT
    assert aggregator_run.execution_end_time == TEST_DT_END
    assert aggregator_run.aggregated_articles_ref.as_dict() == refs
    assert aggregator_run.aggregated_articles_count == 10
    assert aggregator_run.run_status == AggregatorRunStatus.IN_PROGRESS
    assert aggregator_run.sourcing_run_id == AGGREGATION_NOT_SOURCED_IDENTIFIER


def test_sourced_articles_init():
    sourced_article = SourcedArticles(
        topic_id="topic_id",
        sourced_article_id="sourced_article_id",
        dt_sourced=TEST_DT,
        dt_published=TEST_DT_END,
        date_published=TEST_DATE_STR,
        title="title",
        topic="topic",
        labeled_category="labeled_category",
        source_article_ids={"source_article_ids"},
        providers={"cnn", "fox"},
        short_summary_ref="short_summary_ref",
        medium_summary_ref="medium_summary_ref",
        full_summary_ref="full_summary_ref",
        aggregation_run_id="aggregation_run_id",
    )
    assert sourced_article.topic_id == "topic_id"
    assert sourced_article.sourced_article_id == "sourced_article_id"
    assert sourced_article.dt_sourced == TEST_DT
    assert sourced_article.dt_published == TEST_DT_END
    assert sourced_article.date_published == TEST_DATE_STR
    assert sourced_article.title == "title"
    assert sourced_article.topic == "topic"
    assert sourced_article.labeled_category == "labeled_category"
    assert sourced_article.source_article_ids == {"source_article_ids"}
    assert sourced_article.providers == {"cnn", "fox"}
    assert sourced_article.short_summary_ref == "short_summary_ref"
    assert sourced_article.medium_summary_ref == "medium_summary_ref"
    assert sourced_article.full_summary_ref == "full_summary_ref"
    assert sourced_article.thumbs_up == 0
    assert sourced_article.thumbs_down == 0
    assert sourced_article.article_approval_status == ArticleApprovalStatus.PENDING
    assert sourced_article.aggregation_run_id == "aggregation_run_id"
