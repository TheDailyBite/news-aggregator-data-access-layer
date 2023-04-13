from datetime import datetime
from unittest import mock

import pytest

from news_aggregator_data_access_layer.models.dynamodb import (
    AggregatorRuns,
    AggregatorRunStatus,
    ResultRefTypes,
    TrustedNewsProviders,
    UserTopics,
)

TEST_DT = datetime(2023, 4, 11, 21, 2, 39, 4166)
TEST_DT_END = datetime(2023, 4, 11, 22, 2, 49, 4166)


def test_user_topics_init():
    user_topics = UserTopics(
        user_id="user_id",
        topic="topic",
        categories=["category"],
        is_active=True,
        date_created=TEST_DT,
        max_aggregator_results=10,
    )
    assert user_topics.user_id == "user_id"
    assert user_topics.topic == "topic"
    assert user_topics.categories == ["category"]
    assert user_topics.is_active == True
    assert user_topics.date_created == TEST_DT
    assert user_topics.max_aggregator_results == 10


def test_trusted_news_providers_init():
    trusted_news_providers = TrustedNewsProviders(
        provider_name="provider_name",
        provider_url="provider_url",
        trust_score=60,
        provider_aliases=["provider_alias_1", "provider_alias_2"],
    )
    assert trusted_news_providers.provider_name == "provider_name"
    assert trusted_news_providers.provider_url == "provider_url"
    assert trusted_news_providers.trust_score == 60
    assert trusted_news_providers.provider_aliases == ["provider_alias_1", "provider_alias_2"]


def test_aggregator_runs_init_default_vals():
    aggregator_runs = AggregatorRuns(
        aggregator_id="aggregator_id",
        run_datetime=TEST_DT,
        run_end_time=TEST_DT_END,
    )
    assert aggregator_runs.aggregator_id == "aggregator_id"
    assert aggregator_runs.run_datetime == TEST_DT
    assert aggregator_runs.run_status == AggregatorRunStatus.IN_PROGRESS
    assert aggregator_runs.run_end_time == TEST_DT_END
    assert aggregator_runs.result_ref is None


def test_aggregator_runs_init():
    refs = {"type": ResultRefTypes.S3, "bucket": "bucket", "key": "key"}
    aggregator_runs = AggregatorRuns(
        aggregator_id="aggregator_id",
        run_datetime=TEST_DT,
        run_end_time=TEST_DT_END,
        run_status=AggregatorRunStatus.COMPLETE,
        result_ref=refs,
    )
    assert aggregator_runs.aggregator_id == "aggregator_id"
    assert aggregator_runs.run_datetime == TEST_DT
    assert aggregator_runs.run_status == AggregatorRunStatus.COMPLETE
    assert aggregator_runs.run_end_time == TEST_DT_END
    assert aggregator_runs.result_ref.as_dict() == refs
