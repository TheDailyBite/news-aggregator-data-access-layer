import copy
import json
from datetime import datetime
from unittest import mock

import pytest

from news_aggregator_data_access_layer.assets.news_assets import CandidateArticles, RawArticle
from news_aggregator_data_access_layer.config import CANDIDATE_ARTICLES_S3_BUCKET
from news_aggregator_data_access_layer.constants import ALL_CATEGORIES_STR, ResultRefTypes
from news_aggregator_data_access_layer.utils.s3 import dt_to_lexicographic_s3_prefix

TEST_DT = datetime(2023, 4, 11, 21, 2, 39, 4166)
TEST_PUBLISHED_DATE_STR = "2023-04-11T21:02:39+00:00"
TEST_DT_STR = dt_to_lexicographic_s3_prefix(TEST_DT)


def test_raw_article():
    raw_article = RawArticle(
        article_id="article_id",
        aggregator_id="aggregator_id",
        date_published=TEST_PUBLISHED_DATE_STR,
        aggregation_index=0,
        topic="topic",
        title="the article title",
        url="url",
        article_data="article_data",
        sorting="date",
    )
    assert raw_article.article_id == "article_id"
    assert raw_article.aggregator_id == "aggregator_id"
    assert raw_article.date_published == TEST_PUBLISHED_DATE_STR
    assert raw_article.aggregation_index == 0
    assert raw_article.topic == "topic"
    assert raw_article.url == "url"
    assert raw_article.title == "the article title"
    assert raw_article.article_data == "article_data"
    assert raw_article.sorting == "date"
    assert raw_article.discovered_topic == ""
    assert raw_article.requested_category == ALL_CATEGORIES_STR
    assert raw_article.category == ALL_CATEGORIES_STR


def test_raw_article_parse_raw():
    raw_article = RawArticle.parse_raw(
        json.dumps(
            {
                "article_id": "article_id",
                "aggregator_id": "aggregator_id",
                "date_published": TEST_PUBLISHED_DATE_STR,
                "aggregation_index": 0,
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
    assert raw_article.topic == "topic"
    assert raw_article.url == "url"
    assert raw_article.article_data == "article_data"
    assert raw_article.sorting == "date"
    assert raw_article.title == "the article title"
    assert raw_article.discovered_topic == ""
    assert raw_article.requested_category == ALL_CATEGORIES_STR
    assert raw_article.category == ALL_CATEGORIES_STR
    assert raw_article.date_published == TEST_PUBLISHED_DATE_STR
    assert raw_article.aggregation_index == 0


def test_raw_article_parse_raw_with_optional():
    raw_article = RawArticle.parse_raw(
        json.dumps(
            {
                "article_id": "article_id",
                "aggregator_id": "aggregator_id",
                "date_published": TEST_PUBLISHED_DATE_STR,
                "aggregation_index": 0,
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
    assert raw_article.date_published == TEST_PUBLISHED_DATE_STR
    assert raw_article.aggregation_index == 0
    assert raw_article.topic == "topic"
    assert raw_article.url == "url"
    assert raw_article.title == "the article title"
    assert raw_article.article_data == "article_data"
    assert raw_article.sorting == "date"
    assert raw_article.discovered_topic == "some_discovered_topic"
    assert raw_article.category == "some_category"
    assert raw_article.requested_category == "some_requested_category"


def test_candidate_articles_init():
    candidate_articles = CandidateArticles(result_ref_type=ResultRefTypes.S3, candidate_dt=TEST_DT)
    assert candidate_articles.result_ref_type == ResultRefTypes.S3
    assert candidate_articles.candidate_dt == TEST_DT
    assert candidate_articles.candidate_date_str == "2023/04/11"
    assert candidate_articles.candidate_articles == []
    assert candidate_articles.candidate_article_s3_extension == ".json"


def test_candidate_articles_load_articles():
    candidate_articles = CandidateArticles(result_ref_type=ResultRefTypes.S3, candidate_dt=TEST_DT)
    raw_article_1_key = "2023/04/11/21/02/39/004166/article_id.json"
    raw_article_1 = RawArticle(
        article_id="article_id",
        aggregator_id="aggregator_id",
        date_published=TEST_PUBLISHED_DATE_STR,
        aggregation_index=0,
        topic="topic",
        title="the article title",
        url="url",
        article_data="article_data",
        sorting="date",
    )
    raw_article_2_key = "2023/04/11/21/02/39/004166/article_id 2.json"
    raw_article_2 = RawArticle(
        article_id="article_id 2",
        aggregator_id="aggregator_id",
        date_published=TEST_PUBLISHED_DATE_STR,
        aggregation_index=1,
        topic="topic 2",
        title="the article title 2",
        url="url 2",
        article_data="article_data 2",
        sorting="date",
    )
    raw_articles = [(raw_article_1_key, raw_article_1), (raw_article_2_key, raw_article_2)]
    with mock.patch.object(
        candidate_articles, "_load_articles_from_s3", return_value=raw_articles
    ) as mock_load_articles_from_s3:
        kwargs = {"some_key": "some_value"}
        expected_result = [raw_article_1, raw_article_2]
        actual_result = candidate_articles.load_articles(**kwargs)
        mock_load_articles_from_s3.assert_called_once_with(**kwargs)
        assert actual_result == expected_result


def test_candidate_articles_load_articles_raises_not_implemented_error():
    result_ref_type = "Not supported type"
    candidate_articles = CandidateArticles(result_ref_type=result_ref_type, candidate_dt=TEST_DT)  # type: ignore
    with pytest.raises(NotImplementedError) as exc_info:
        kwargs = {"some_key": "some_value"}
        actual_result = candidate_articles.load_articles(**kwargs)
        assert str(exc_info.value) == f"Result reference type {result_ref_type} not implemented"


def test_candidate_articles_load_articles_from_s3():
    with mock.patch(
        "news_aggregator_data_access_layer.assets.news_assets.read_objects_from_prefix_with_extension"
    ) as mock_read_objects:
        with mock.patch(
            "news_aggregator_data_access_layer.assets.news_assets.get_success_file",
            return_value=("body", {"some_key": "some_value"}),
        ) as mock_get_success_file:
            candidate_articles = CandidateArticles(
                result_ref_type=ResultRefTypes.S3, candidate_dt=TEST_DT
            )
            raw_article_1_key = "2023/04/11/21/02/39/004166/article_id.json"
            raw_article_1 = RawArticle(
                article_id="article_id",
                aggregator_id="aggregator_id",
                date_published=TEST_PUBLISHED_DATE_STR,
                aggregation_index=0,
                topic="topic",
                title="the article title",
                url="url",
                article_data="article_data",
                sorting="date",
            )
            raw_article_2_key = "2023/04/11/21/02/39/004166/article_id 2.json"
            raw_article_2 = RawArticle(
                article_id="article_id 2",
                aggregator_id="aggregator_id",
                date_published=TEST_PUBLISHED_DATE_STR,
                aggregation_index=1,
                topic="topic 2",
                title="the article title 2",
                url="url 2",
                article_data="article_data 2",
                sorting="date",
            )
            expected_result = [
                (raw_article_1_key, raw_article_1),
                (raw_article_2_key, raw_article_2),
            ]
            raw_articles = [
                (raw_article_1_key, raw_article_1.json()),
                (raw_article_2_key, raw_article_2.json()),
            ]
            mock_read_objects.return_value = raw_articles
            test_s3_client = "test_s3_client"
            test_topic = "test_topic"
            expected_prefix = candidate_articles._get_raw_candidates_s3_object_prefix(test_topic)
            kwargs = {
                "s3_client": test_s3_client,
                "topic": test_topic,
            }
            actual_result = candidate_articles._load_articles_from_s3(**kwargs)
            mock_read_objects.assert_called_once_with(
                CANDIDATE_ARTICLES_S3_BUCKET,
                expected_prefix,
                candidate_articles.candidate_article_s3_extension,
                candidate_articles.success_marker_fn,
                check_success_file=True,
                s3_client=test_s3_client,
            )
            assert actual_result == expected_result


def test_candidate_articles_store_articles():
    prefix = "some_prefix"
    candidate_articles = CandidateArticles(result_ref_type=ResultRefTypes.S3, candidate_dt=TEST_DT)
    raw_article_1 = RawArticle(
        article_id="article_id",
        aggregator_id="aggregator_id",
        date_published=TEST_PUBLISHED_DATE_STR,
        aggregation_index=0,
        topic="topic",
        title="the article title",
        url="url",
        article_data="article_data",
        sorting="date",
    )
    raw_article_2 = RawArticle(
        article_id="article_id 2",
        aggregator_id="aggregator_id",
        date_published=TEST_PUBLISHED_DATE_STR,
        aggregation_index=1,
        topic="topic 2",
        title="the article title 2",
        url="url 2",
        article_data="article_data 2",
        sorting="date",
    )
    raw_articles = [raw_article_1, raw_article_2]
    result = (CANDIDATE_ARTICLES_S3_BUCKET, prefix)
    with mock.patch.object(
        candidate_articles, "_store_articles_in_s3"
    ) as mock_store_articles_in_s3:
        kwargs = {
            "s3_client": "s3_client",
            "topic": "topic",
            "aggregator_id": "bing",
            "aggregation_dt": TEST_DT,
            "articles": raw_articles,
        }
        mock_store_articles_in_s3.return_value = result
        expected_result = result
        actual_result = candidate_articles.store_articles(**kwargs)
        mock_store_articles_in_s3.assert_called_once_with(**kwargs)
        assert actual_result == expected_result


def test_candidate_articles_store_articles_in_s3_success_file_not_exists():
    with mock.patch(
        "news_aggregator_data_access_layer.assets.news_assets.store_object_in_s3"
    ) as mock_store_objects:
        with mock.patch(
            "news_aggregator_data_access_layer.assets.news_assets.success_file_exists_at_prefix",
            return_value=False,
        ) as mock_success_file_exists:
            with mock.patch(
                "news_aggregator_data_access_layer.assets.news_assets.store_success_file"
            ) as mock_store_success_file:
                candidate_articles = CandidateArticles(
                    result_ref_type=ResultRefTypes.S3, candidate_dt=TEST_DT
                )
                raw_article_1 = RawArticle(
                    article_id="article_id",
                    aggregator_id="aggregator_id",
                    date_published=TEST_PUBLISHED_DATE_STR,
                    aggregation_index=0,
                    topic="topic",
                    title="the article title",
                    url="url",
                    article_data="article_data",
                    sorting="date",
                )
                raw_article_2 = RawArticle(
                    article_id="article_id 2",
                    aggregator_id="aggregator_id",
                    date_published=TEST_PUBLISHED_DATE_STR,
                    aggregation_index=1,
                    topic="topic 2",
                    title="the article title 2",
                    url="url 2",
                    article_data="article_data 2",
                    sorting="date",
                )
                raw_articles = [raw_article_1, raw_article_2]
                test_s3_client = "test_s3_client"
                test_topic = "test_topic"
                test_aggregator_id = "test_aggregator_id"
                prefix = candidate_articles._get_raw_candidates_s3_object_prefix(test_topic)
                raw_article_1_key = candidate_articles._get_raw_article_s3_object_key(
                    test_topic, raw_article_1.article_id
                )
                raw_article_2_key = candidate_articles._get_raw_article_s3_object_key(
                    test_topic, raw_article_2.article_id
                )
                kwargs = {
                    "s3_client": test_s3_client,
                    "topic": test_topic,
                    "aggregator_id": test_aggregator_id,
                    "aggregation_dt": TEST_DT,
                    "articles": raw_articles,
                }
                expected_result = (CANDIDATE_ARTICLES_S3_BUCKET, prefix)
                expected_object_metadata = copy.deepcopy(
                    candidate_articles.default_success_file_metadata
                )
                expected_object_metadata[
                    candidate_articles.success_metadata_aggregators_key
                ] = test_aggregator_id
                expected_object_metadata[
                    candidate_articles.success_metadata_aggregators_dt_key
                ] = TEST_DT_STR
                actual_result = candidate_articles._store_articles_in_s3(**kwargs)
                mock_store_objects.assert_has_calls(
                    [
                        mock.call(
                            CANDIDATE_ARTICLES_S3_BUCKET,
                            raw_article_1_key,
                            raw_article_1.json(),
                            object_metadata=dict(),
                            overwrite_allowed=False,
                            s3_client=test_s3_client,
                        ),
                        mock.call(
                            CANDIDATE_ARTICLES_S3_BUCKET,
                            raw_article_2_key,
                            raw_article_2.json(),
                            object_metadata=dict(),
                            overwrite_allowed=False,
                            s3_client=test_s3_client,
                        ),
                    ]
                )
                mock_store_success_file.assert_called_once_with(
                    CANDIDATE_ARTICLES_S3_BUCKET,
                    prefix,
                    candidate_articles.success_marker_fn,
                    object_metadata=expected_object_metadata,
                    s3_client=test_s3_client,
                )
                assert actual_result == expected_result


def test_candidate_articles_store_articles_in_s3_success_file_exists():
    with mock.patch(
        "news_aggregator_data_access_layer.assets.news_assets.store_object_in_s3"
    ) as mock_store_objects:
        with mock.patch(
            "news_aggregator_data_access_layer.assets.news_assets.success_file_exists_at_prefix",
            return_value=True,
        ) as mock_success_file_exists:
            with mock.patch(
                "news_aggregator_data_access_layer.assets.news_assets.get_success_file"
            ) as mock_get_success_file:
                with mock.patch(
                    "news_aggregator_data_access_layer.assets.news_assets.store_success_file"
                ) as mock_store_success_file:
                    candidate_articles = CandidateArticles(
                        result_ref_type=ResultRefTypes.S3, candidate_dt=TEST_DT
                    )
                    existing_success_file_metadata = (
                        candidate_articles.default_success_file_metadata
                    )
                    existing_success_file_metadata[
                        candidate_articles.success_metadata_aggregators_key
                    ] = "bing"
                    existing_success_file_metadata[
                        candidate_articles.success_metadata_aggregators_dt_key
                    ] = TEST_DT_STR
                    mock_get_success_file.return_value = (
                        "some_body",
                        existing_success_file_metadata,
                    )
                    raw_article_1 = RawArticle(
                        article_id="article_id",
                        aggregator_id="aggregator_id",
                        date_published=TEST_PUBLISHED_DATE_STR,
                        aggregation_index=0,
                        topic="topic",
                        title="the article title",
                        url="url",
                        article_data="article_data",
                        sorting="date",
                    )
                    raw_article_2 = RawArticle(
                        article_id="article_id 2",
                        aggregator_id="aggregator_id",
                        date_published=TEST_PUBLISHED_DATE_STR,
                        aggregation_index=1,
                        topic="topic 2",
                        title="the article title 2",
                        url="url 2",
                        article_data="article_data 2",
                        sorting="date",
                    )
                    raw_articles = [raw_article_1, raw_article_2]
                    test_s3_client = "test_s3_client"
                    test_topic = "test_topic"
                    test_aggregator_id = "test_aggregator_id"
                    prefix = candidate_articles._get_raw_candidates_s3_object_prefix(test_topic)
                    raw_article_1_key = candidate_articles._get_raw_article_s3_object_key(
                        test_topic, raw_article_1.article_id
                    )
                    raw_article_2_key = candidate_articles._get_raw_article_s3_object_key(
                        test_topic, raw_article_2.article_id
                    )
                    kwargs = {
                        "s3_client": test_s3_client,
                        "topic": test_topic,
                        "aggregator_id": test_aggregator_id,
                        "aggregation_dt": TEST_DT,
                        "articles": raw_articles,
                    }
                    expected_result = (CANDIDATE_ARTICLES_S3_BUCKET, prefix)
                    expected_object_metadata = copy.deepcopy(existing_success_file_metadata)
                    expected_object_metadata[
                        candidate_articles.success_metadata_aggregators_key
                    ] = f"bing,{test_aggregator_id}"
                    expected_object_metadata[
                        candidate_articles.success_metadata_aggregators_dt_key
                    ] = f"{TEST_DT_STR},{TEST_DT_STR}"
                    actual_result = candidate_articles._store_articles_in_s3(**kwargs)
                    mock_store_objects.assert_has_calls(
                        [
                            mock.call(
                                CANDIDATE_ARTICLES_S3_BUCKET,
                                raw_article_1_key,
                                raw_article_1.json(),
                                object_metadata=dict(),
                                overwrite_allowed=False,
                                s3_client=test_s3_client,
                            ),
                            mock.call(
                                CANDIDATE_ARTICLES_S3_BUCKET,
                                raw_article_2_key,
                                raw_article_2.json(),
                                object_metadata=dict(),
                                overwrite_allowed=False,
                                s3_client=test_s3_client,
                            ),
                        ]
                    )
                    mock_store_success_file.assert_called_once_with(
                        CANDIDATE_ARTICLES_S3_BUCKET,
                        prefix,
                        candidate_articles.success_marker_fn,
                        object_metadata=expected_object_metadata,
                        s3_client=test_s3_client,
                    )
                    assert actual_result == expected_result
