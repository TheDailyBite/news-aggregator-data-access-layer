from typing import List, Tuple

import copy
import json
from collections.abc import Mapping
from datetime import datetime
from unittest import mock

import pytest

from news_aggregator_data_access_layer.assets.news_assets import CandidateArticles, RawArticle
from news_aggregator_data_access_layer.config import CANDIDATE_ARTICLES_S3_BUCKET
from news_aggregator_data_access_layer.constants import ALL_CATEGORIES_STR, ResultRefTypes
from news_aggregator_data_access_layer.utils.s3 import dt_to_lexicographic_s3_prefix

TEST_DT = datetime(2023, 4, 11, 21, 2, 39, 4166)
TEST_PUBLISHED_ISO_DT = "2023-04-11T21:02:39+00:00"
TEST_PUBLISHED_ISO_DT_2 = "2023-05-11T21:02:39+00:00"
TEST_PUBLISHED_DATE = "2023/04/11"
TEST_PUBLISHED_DATE_2 = "2023/05/11"
TEST_DT_STR = dt_to_lexicographic_s3_prefix(TEST_DT)
TEST_AGGREGATOR_RUN_ID = "23a0b9db-7a43-48d2-98e7-819a8f885c2e"
TEST_AGGREGATOR_ID = "test_aggregator_id"
TEST_TOPIC_ID = "test_topic_id"


def test_raw_article():
    raw_article = RawArticle(
        article_id="article_id",
        aggregator_id="aggregator_id",
        dt_published=TEST_PUBLISHED_ISO_DT,
        aggregation_index=0,
        topic_id=TEST_TOPIC_ID,
        topic="topic",
        title="the article title",
        url="url",
        article_data="article_data",
        sorting="date",
    )
    assert raw_article.article_id == "article_id"
    assert raw_article.aggregator_id == "aggregator_id"
    assert raw_article.dt_published == TEST_PUBLISHED_ISO_DT
    assert raw_article.aggregation_index == 0
    assert raw_article.topic == "topic"
    assert raw_article.url == "url"
    assert raw_article.title == "the article title"
    assert raw_article.article_data == "article_data"
    assert raw_article.sorting == "date"
    assert raw_article.discovered_topic == ""
    assert raw_article.requested_category == ALL_CATEGORIES_STR
    assert raw_article.category == ALL_CATEGORIES_STR
    assert raw_article.topic_id == TEST_TOPIC_ID


def test_raw_article_process_data_with_provider_domain_no_article_processed_data():
    raw_article = RawArticle(
        article_id="article_id",
        aggregator_id="aggregator_id",
        dt_published=TEST_PUBLISHED_ISO_DT,
        aggregation_index=0,
        topic_id=TEST_TOPIC_ID,
        topic="topic",
        title="the article title",
        url="https://www.inc.com/sania-khan/invalid-article.html",
        article_data="article_data",
        sorting="date",
    )
    raw_article.process_article_data()
    assert raw_article.article_id == "article_id"
    assert raw_article.aggregator_id == "aggregator_id"
    assert raw_article.dt_published == TEST_PUBLISHED_ISO_DT
    assert raw_article.aggregation_index == 0
    assert raw_article.topic_id == TEST_TOPIC_ID
    assert raw_article.topic == "topic"
    assert raw_article.url == "https://www.inc.com/sania-khan/invalid-article.html"
    assert raw_article.title == "the article title"
    assert raw_article.article_data == "article_data"
    assert raw_article.sorting == "date"
    assert raw_article.discovered_topic == ""
    assert raw_article.requested_category == ALL_CATEGORIES_STR
    assert raw_article.category == ALL_CATEGORIES_STR
    assert raw_article.provider_domain == "inc"
    assert raw_article.article_processed_data == ""


def test_raw_article_parse_raw():
    raw_article = RawArticle.parse_raw(
        json.dumps(
            {
                "article_id": "article_id",
                "aggregator_id": "aggregator_id",
                "dt_published": TEST_PUBLISHED_ISO_DT,
                "aggregation_index": 0,
                "topic_id": TEST_TOPIC_ID,
                "topic": "topic",
                "title": "the article title",
                "url": "url",
                "article_data": "article_data",
                "sorting": "date",
            }
        )
    )
    assert raw_article.article_id == "article_id"
    assert raw_article.aggregator_id == "aggregator_id"
    assert raw_article.topic_id == TEST_TOPIC_ID
    assert raw_article.topic == "topic"
    assert raw_article.url == "url"
    assert raw_article.article_data == "article_data"
    assert raw_article.sorting == "date"
    assert raw_article.title == "the article title"
    assert raw_article.discovered_topic == ""
    assert raw_article.requested_category == ALL_CATEGORIES_STR
    assert raw_article.category == ALL_CATEGORIES_STR
    assert raw_article.dt_published == TEST_PUBLISHED_ISO_DT
    assert raw_article.aggregation_index == 0


def test_raw_article_parse_raw_with_optional():
    raw_article = RawArticle.parse_raw(
        json.dumps(
            {
                "article_id": "article_id",
                "aggregator_id": "aggregator_id",
                "dt_published": TEST_PUBLISHED_ISO_DT,
                "aggregation_index": 0,
                "topic_id": TEST_TOPIC_ID,
                "topic": "topic",
                "title": "the article title",
                "url": "url",
                "article_data": "article_data",
                "sorting": "date",
                "discovered_topic": "some_discovered_topic",
                "requested_category": "some_requested_category",
                "category": "some_category",
            }
        )
    )
    assert raw_article.article_id == "article_id"
    assert raw_article.aggregator_id == "aggregator_id"
    assert raw_article.dt_published == TEST_PUBLISHED_ISO_DT
    assert raw_article.aggregation_index == 0
    assert raw_article.topic_id == TEST_TOPIC_ID
    assert raw_article.topic == "topic"
    assert raw_article.url == "url"
    assert raw_article.title == "the article title"
    assert raw_article.article_data == "article_data"
    assert raw_article.sorting == "date"
    assert raw_article.discovered_topic == "some_discovered_topic"
    assert raw_article.category == "some_category"
    assert raw_article.requested_category == "some_requested_category"


def test_candidate_articles_init():
    candidate_articles = CandidateArticles(
        result_ref_type=ResultRefTypes.S3,
        topic_id=TEST_TOPIC_ID,
    )
    assert candidate_articles.result_ref_type == ResultRefTypes.S3
    assert candidate_articles.candidate_articles == []
    assert candidate_articles.candidate_article_s3_extension == ".json"
    assert candidate_articles.topic_id == TEST_TOPIC_ID


def test_candidate_articles__get_raw_candidates_s3_object_prefix():
    raw_article = RawArticle.parse_raw(
        json.dumps(
            {
                "article_id": "article_id",
                "aggregator_id": "aggregator_id",
                "dt_published": TEST_PUBLISHED_ISO_DT,
                "aggregation_index": 0,
                "topic_id": TEST_TOPIC_ID,
                "topic": "topic",
                "title": "the article title",
                "url": "url",
                "article_data": "article_data",
                "sorting": "date",
                "discovered_topic": "some_discovered_topic",
                "requested_category": "some_requested_category",
                "category": "some_category",
            }
        )
    )
    candidate_articles = CandidateArticles(
        result_ref_type=ResultRefTypes.S3,
        topic_id=TEST_TOPIC_ID,
    )
    expected_object_key = f"raw_candidate_articles/{TEST_PUBLISHED_DATE}/{TEST_TOPIC_ID}/{raw_article.article_id}.json"
    actual_object_key = candidate_articles._get_raw_article_s3_object_key(raw_article)
    assert actual_object_key == expected_object_key


def test_candidate_articles__get_raw_article_s3_object_key():
    candidate_articles = CandidateArticles(
        result_ref_type=ResultRefTypes.S3,
        topic_id=TEST_TOPIC_ID,
    )
    expected_prefix = f"raw_candidate_articles/{TEST_PUBLISHED_DATE}/{TEST_TOPIC_ID}"
    actual_prefix = candidate_articles._get_raw_candidates_s3_object_prefix(TEST_PUBLISHED_DATE)
    assert actual_prefix == expected_prefix


def test_candidate_articles_load_articles():
    candidate_articles = CandidateArticles(
        result_ref_type=ResultRefTypes.S3,
        topic_id=TEST_TOPIC_ID,
    )
    raw_article_1_key = "2023/04/11/21/02/39/004166/article_id.json"
    raw_article_1 = RawArticle(
        article_id="article_id",
        aggregator_id="aggregator_id",
        dt_published=TEST_PUBLISHED_ISO_DT,
        topic_id=TEST_TOPIC_ID,
        aggregation_index=0,
        topic="topic",
        title="the article title",
        url="url",
        article_data="article_data",
        sorting="date",
    )
    raw_article_1_metadata = {
        candidate_articles.aggregation_run_id_metadata_key: TEST_AGGREGATOR_RUN_ID,
        candidate_articles.aggregator_id_metadata_key: "aggregator_id",
    }
    raw_article_1_tags = {
        candidate_articles.is_sourced_article_tag_key: "False",
    }
    raw_article_2_key = "2023/04/11/21/02/39/004166/article_id 2.json"
    raw_article_2 = RawArticle(
        article_id="article_id 2",
        aggregator_id="aggregator_id",
        dt_published=TEST_PUBLISHED_ISO_DT,
        topic_id=TEST_TOPIC_ID,
        aggregation_index=1,
        topic="topic 2",
        title="the article title 2",
        url="url 2",
        article_data="article_data 2",
        sorting="date",
    )
    raw_article_2_metadata = {
        candidate_articles.aggregation_run_id_metadata_key: TEST_AGGREGATOR_RUN_ID,
        candidate_articles.aggregator_id_metadata_key: "aggregator_id",
    }
    raw_article_2_tags = {
        candidate_articles.is_sourced_article_tag_key: "False",
    }
    raw_articles = [
        (raw_article_1_key, raw_article_1, raw_article_1_metadata, raw_article_1_tags),
        (raw_article_2_key, raw_article_2, raw_article_2_metadata, raw_article_2_tags),
    ]
    with mock.patch.object(
        candidate_articles, "_load_articles_from_s3", return_value=raw_articles
    ) as mock_load_articles_from_s3:
        kwargs = {"some_key": "some_value"}
        expected_result = [
            (raw_article_1, raw_article_1_metadata, raw_article_1_tags),
            (raw_article_2, raw_article_2_metadata, raw_article_2_tags),
        ]
        actual_result = candidate_articles.load_articles(**kwargs)
        mock_load_articles_from_s3.assert_called_once_with(**kwargs)
        assert actual_result == expected_result


def test_candidate_articles_load_articles_filter_is_sourced():
    candidate_articles = CandidateArticles(
        result_ref_type=ResultRefTypes.S3,
        topic_id=TEST_TOPIC_ID,
    )
    raw_article_1_key = "2023/04/11/21/02/39/004166/article_id.json"
    raw_article_1 = RawArticle(
        article_id="article_id",
        aggregator_id="aggregator_id",
        dt_published=TEST_PUBLISHED_ISO_DT,
        topic_id=TEST_TOPIC_ID,
        aggregation_index=0,
        topic="topic",
        title="the article title",
        url="url",
        article_data="article_data",
        sorting="date",
    )
    raw_article_1_metadata = {
        candidate_articles.aggregation_run_id_metadata_key: TEST_AGGREGATOR_RUN_ID,
        candidate_articles.aggregator_id_metadata_key: "aggregator_id",
    }
    raw_article_1_tags = {
        candidate_articles.is_sourced_article_tag_key: "False",
    }
    raw_article_2_key = "2023/04/11/21/02/39/004166/article_id 2.json"
    raw_article_2 = RawArticle(
        article_id="article_id 2",
        aggregator_id="aggregator_id",
        dt_published=TEST_PUBLISHED_ISO_DT,
        topic_id=TEST_TOPIC_ID,
        aggregation_index=1,
        topic="topic 2",
        title="the article title 2",
        url="url 2",
        article_data="article_data 2",
        sorting="date",
    )
    raw_article_2_metadata = {
        candidate_articles.aggregation_run_id_metadata_key: TEST_AGGREGATOR_RUN_ID,
        candidate_articles.aggregator_id_metadata_key: "aggregator_id",
    }
    raw_article_2_tags = {
        candidate_articles.is_sourced_article_tag_key: "True",
    }
    raw_articles = [
        (raw_article_1_key, raw_article_1, raw_article_1_metadata, raw_article_1_tags),
        (raw_article_2_key, raw_article_2, raw_article_2_metadata, raw_article_2_tags),
    ]
    with mock.patch.object(
        candidate_articles, "_load_articles_from_s3", return_value=raw_articles
    ) as mock_load_articles_from_s3:
        kwargs = {"some_key": "some_value"}
        expected_result = [
            (raw_article_1, raw_article_1_metadata, raw_article_1_tags),
        ]
        actual_result = candidate_articles.load_articles(
            tag_filter_key=candidate_articles.is_sourced_article_tag_key,
            tag_filter_value="False",
            **kwargs,
        )
        mock_load_articles_from_s3.assert_called_once_with(**kwargs)
        assert actual_result == expected_result


def test_candidate_articles_load_articles_filter_is_sourced_no_results():
    candidate_articles = CandidateArticles(
        result_ref_type=ResultRefTypes.S3,
        topic_id=TEST_TOPIC_ID,
    )
    raw_article_1_key = "2023/04/11/21/02/39/004166/article_id.json"
    raw_article_1 = RawArticle(
        article_id="article_id",
        aggregator_id="aggregator_id",
        dt_published=TEST_PUBLISHED_ISO_DT,
        topic_id=TEST_TOPIC_ID,
        aggregation_index=0,
        topic="topic",
        title="the article title",
        url="url",
        article_data="article_data",
        sorting="date",
    )
    raw_article_1_metadata = {
        candidate_articles.aggregation_run_id_metadata_key: TEST_AGGREGATOR_RUN_ID,
        candidate_articles.aggregator_id_metadata_key: "aggregator_id",
    }
    raw_article_1_tags = {
        candidate_articles.is_sourced_article_tag_key: "False",
    }
    raw_article_2_key = "2023/04/11/21/02/39/004166/article_id 2.json"
    raw_article_2 = RawArticle(
        article_id="article_id 2",
        aggregator_id="aggregator_id",
        dt_published=TEST_PUBLISHED_ISO_DT,
        topic_id=TEST_TOPIC_ID,
        aggregation_index=1,
        topic="topic 2",
        title="the article title 2",
        url="url 2",
        article_data="article_data 2",
        sorting="date",
    )
    raw_article_2_metadata = {
        candidate_articles.is_sourced_article_tag_key: "True",
        candidate_articles.aggregation_run_id_metadata_key: TEST_AGGREGATOR_RUN_ID,
        candidate_articles.aggregator_id_metadata_key: "aggregator_id",
    }
    raw_article_2_tags = {
        candidate_articles.is_sourced_article_tag_key: "True",
    }
    raw_articles = [
        (raw_article_1_key, raw_article_1, raw_article_1_metadata, raw_article_1_tags),
        (raw_article_2_key, raw_article_2, raw_article_2_metadata, raw_article_2_tags),
    ]
    with mock.patch.object(
        candidate_articles, "_load_articles_from_s3", return_value=raw_articles
    ) as mock_load_articles_from_s3:
        kwargs = {"some_key": "some_value"}
        expected_result: list[tuple[RawArticle, Mapping[str, str], Mapping[str, str]]] = []
        actual_result = candidate_articles.load_articles(
            tag_filter_key=candidate_articles.is_sourced_article_tag_key,
            tag_filter_value="Invalid Value",
            **kwargs,
        )
        mock_load_articles_from_s3.assert_called_once_with(**kwargs)
        assert actual_result == expected_result


def test_candidate_articles_load_articles_raises_not_implemented_error():
    result_ref_type = "Not supported type"
    candidate_articles = CandidateArticles(result_ref_type=result_ref_type, topic_id=TEST_TOPIC_ID)  # type: ignore
    with pytest.raises(NotImplementedError) as exc_info:
        kwargs = {"some_key": "some_value", "s3_client": "some-client"}
        actual_result = candidate_articles.load_articles(**kwargs)
        assert str(exc_info.value) == f"Result reference type {result_ref_type} not implemented"


def test_candidate_articles_load_articles_from_s3():
    with mock.patch(
        "news_aggregator_data_access_layer.assets.news_assets.read_objects_from_prefix_with_extension"
    ) as mock_read_objects:
        candidate_articles = CandidateArticles(
            result_ref_type=ResultRefTypes.S3,
            topic_id=TEST_TOPIC_ID,
        )
        raw_article_1_key = "2023/04/11/21/02/39/004166/article_id.json"
        raw_article_1 = RawArticle(
            article_id="article_id",
            aggregator_id="aggregator_id",
            dt_published=TEST_PUBLISHED_ISO_DT,
            aggregation_index=0,
            topic_id=TEST_TOPIC_ID,
            topic="topic",
            title="the article title",
            url="url",
            article_data="article_data",
            sorting="date",
        )
        raw_article_1_metadata = {
            candidate_articles.aggregation_run_id_metadata_key: TEST_AGGREGATOR_RUN_ID,
            candidate_articles.aggregator_id_metadata_key: "aggregator_id",
        }
        raw_article_1_tags = {
            candidate_articles.is_sourced_article_tag_key: "False",
        }
        raw_article_2_key = "2023/04/11/21/02/39/004166/article_id 2.json"
        raw_article_2 = RawArticle(
            article_id="article_id 2",
            aggregator_id="aggregator_id",
            dt_published=TEST_PUBLISHED_ISO_DT,
            topic_id=TEST_TOPIC_ID,
            aggregation_index=1,
            topic="topic 2",
            title="the article title 2",
            url="url 2",
            article_data="article_data 2",
            sorting="date",
        )
        raw_article_2_metadata = {
            candidate_articles.aggregation_run_id_metadata_key: TEST_AGGREGATOR_RUN_ID,
            candidate_articles.aggregator_id_metadata_key: "aggregator_id",
        }
        raw_article_2_tags = {
            candidate_articles.is_sourced_article_tag_key: "False",
        }
        expected_result = [
            (raw_article_1_key, raw_article_1, raw_article_1_metadata, raw_article_1_tags),
            (raw_article_2_key, raw_article_2, raw_article_2_metadata, raw_article_2_tags),
        ]
        raw_articles = [
            [raw_article_1_key, raw_article_1.json(), raw_article_1_metadata, raw_article_1_tags],
            [raw_article_2_key, raw_article_2.json(), raw_article_2_metadata, raw_article_2_tags],
        ]
        mock_read_objects.return_value = raw_articles
        test_s3_client = "test_s3_client"
        article_published_date = TEST_PUBLISHED_DATE
        expected_prefix = candidate_articles._get_raw_candidates_s3_object_prefix(
            article_published_date
        )
        kwargs = {"s3_client": test_s3_client, "publishing_date": TEST_DT}
        actual_result = candidate_articles._load_articles_from_s3(**kwargs)
        mock_read_objects.assert_called_once_with(
            CANDIDATE_ARTICLES_S3_BUCKET,
            expected_prefix,
            candidate_articles.candidate_article_s3_extension,
            s3_client=test_s3_client,
        )
        assert actual_result == expected_result


def test_candidate_articles_store_articles():
    prefixes = ["prefix1", "prefix2"]
    candidate_articles = CandidateArticles(
        result_ref_type=ResultRefTypes.S3,
        topic_id=TEST_TOPIC_ID,
    )
    raw_article_1 = RawArticle(
        article_id="article_id",
        aggregator_id="aggregator_id",
        dt_published=TEST_PUBLISHED_ISO_DT,
        aggregation_index=0,
        topic_id=TEST_TOPIC_ID,
        topic="topic",
        title="the article title",
        url="url",
        article_data="article_data",
        sorting="date",
    )
    raw_article_2 = RawArticle(
        article_id="article_id 2",
        aggregator_id="aggregator_id",
        dt_published=TEST_PUBLISHED_ISO_DT,
        aggregation_index=1,
        topic_id=TEST_TOPIC_ID,
        topic="topic",
        title="the article title 2",
        url="url 2",
        article_data="article_data 2",
        sorting="date",
    )
    raw_articles = [raw_article_1, raw_article_2]
    result = (CANDIDATE_ARTICLES_S3_BUCKET, prefixes)
    with mock.patch.object(
        candidate_articles, "_store_articles_in_s3"
    ) as mock_store_articles_in_s3:
        kwargs = {
            "s3_client": "s3_client",
            "articles": raw_articles,
            "aggregation_run_id": TEST_AGGREGATOR_RUN_ID,
        }
        mock_store_articles_in_s3.return_value = result
        expected_result = result
        actual_result = candidate_articles.store_articles(**kwargs)
        mock_store_articles_in_s3.assert_called_once_with(**kwargs)
        assert actual_result == expected_result


def test_candidate_articles__store_articles_in_s3():
    candidate_articles = CandidateArticles(
        result_ref_type=ResultRefTypes.S3,
        topic_id=TEST_TOPIC_ID,
    )
    prefixes = [
        candidate_articles._get_raw_candidates_s3_object_prefix(TEST_PUBLISHED_DATE),
        candidate_articles._get_raw_candidates_s3_object_prefix(TEST_PUBLISHED_DATE_2),
    ]
    raw_article_1 = RawArticle(
        article_id="article_id",
        aggregator_id="aggregator_id",
        dt_published=TEST_PUBLISHED_ISO_DT,
        aggregation_index=0,
        topic_id=TEST_TOPIC_ID,
        topic="topic",
        title="the article title",
        url="url",
        article_data="article_data",
        sorting="date",
    )
    raw_article_2 = RawArticle(
        article_id="article_id 2",
        aggregator_id="aggregator_id",
        dt_published=TEST_PUBLISHED_ISO_DT_2,
        aggregation_index=1,
        topic_id=TEST_TOPIC_ID,
        topic="topic",
        title="the article title 2",
        url="url 2",
        article_data="article_data 2",
        sorting="date",
    )
    raw_articles = [raw_article_1, raw_article_2]
    expected_result = (CANDIDATE_ARTICLES_S3_BUCKET, prefixes)
    with mock.patch(
        "news_aggregator_data_access_layer.assets.news_assets.store_object_in_s3"
    ) as mock_store_object_in_s3:
        kwargs = {
            "s3_client": "s3_client",
            "articles": raw_articles,
            "aggregation_run_id": TEST_AGGREGATOR_RUN_ID,
        }
        actual_result = candidate_articles._store_articles_in_s3(**kwargs)
        assert actual_result[0] == expected_result[0]
        assert set(actual_result[1]) == set(expected_result[1])
