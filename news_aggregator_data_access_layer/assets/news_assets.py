from typing import Any, List, Optional, Tuple

import copy
import json
from collections.abc import Mapping
from datetime import datetime

import tldextract
from newsplease import NewsPlease
from pydantic import BaseModel, Field

from news_aggregator_data_access_layer.config import CANDIDATE_ARTICLES_S3_BUCKET
from news_aggregator_data_access_layer.constants import (
    ARTICLE_NOT_SOURCED_TAGS_FLAG,
    ARTICLE_SOURCED_METADATA_FLAG,
    DATE_PUBLISHED_ARTICLE_REGEX,
    DT_LEXICOGRAPHIC_STR_FORMAT,
    ResultRefTypes,
)
from news_aggregator_data_access_layer.utils.s3 import (
    dt_to_lexicographic_date_s3_prefix,
    dt_to_lexicographic_s3_prefix,
    get_success_file,
    read_objects_from_prefix_with_extension,
    store_object_in_s3,
    store_success_file,
    success_file_exists_at_prefix,
)
from news_aggregator_data_access_layer.utils.telemetry import setup_logger

logger = setup_logger(__name__)


class RawArticle(BaseModel):
    article_id: str
    aggregator_id: str
    # iso8601 format with seconds precision
    dt_published: str = Field(regex=DATE_PUBLISHED_ARTICLE_REGEX)
    aggregation_index: int
    topic_id: str
    # this is the search query
    topic: str
    # this is the topic that was discovered by an algo
    discovered_topic: Optional[str] = ""
    requested_category: Optional[str] = ""
    category: Optional[str] = ""
    title: str
    url: str
    article_data: str
    # relevance or date
    sorting: str
    provider_domain: Optional[str] = ""
    article_processed_data: Optional[str] = ""

    def process_article_data(self):
        if self.article_processed_data:
            return self.article_processed_data
        else:
            article = NewsPlease.from_url(self.url)
            ext_res = tldextract.extract(self.url)
            self.provider_domain = ext_res.domain.lower()
            if not article:
                logger.warning(
                    f"Could not process article with url {self.url} and provider domain {self.provider_domain}"
                )
                # TODO - emit metric with domain
                return
            self.article_processed_data = json.dumps(article.get_serializable_dict())


class CandidateArticles:
    def __init__(self, result_ref_type: ResultRefTypes, topic_id: str):
        self.result_ref_type = result_ref_type
        self.topic_id = topic_id
        self.candidate_articles: list[tuple[RawArticle, Mapping[str, str], Mapping[str, str]]] = []
        self.candidate_article_s3_extension = ".json"
        self.success_marker_fn = "__SUCCESS__"
        self.is_sourced_article_tag_key = "is_sourced_article"
        self.aggregation_run_id_metadata_key = "aggregation_run_id"
        self.aggregator_id_metadata_key = "aggregator_id"

    def load_articles(
        self, tag_filter_key: str = "", tag_filter_value: str = "", **kwargs: Any
    ) -> list[tuple[RawArticle, Mapping[str, str], Mapping[str, str]]]:
        """Load raw articles from the appropriate source and filter them based on tags, if necessary

        Args:
            tag_filter_key (str, optional): The tag key for which we will evaluate the value. Defaults to "", which means no filtering will occur.
            tag_filter_value (str, optional): The tag filter value. If the tag key specified (if any) matches this value we will include the item in the results. Defaults to "".
            kwargs (Any): Required kwargs to fetch articles for the appropriate result reference type (see specific methods for details)

        Raises:
            NotImplementedError: If the result reference type is not implemented

        Returns:
            list[Tuple[RawArticle, Mapping[str, str], Mapping[str, str]]]: A list of tuples, possibly filtered, containing the raw article, the metadata, and the tags, if any.
        """
        self.candidate_articles = []
        if self.result_ref_type == ResultRefTypes.S3:
            unsorted_candidate_articles = self._load_articles_from_s3(**kwargs)
            # TODO - implement sorting
            for a in unsorted_candidate_articles:
                raw_article = a[1]
                object_metadata = a[2]
                object_tags = a[3]
                # filter to only exclude non-matching articles
                if tag_filter_key and tag_filter_value:
                    if object_tags[tag_filter_key] != tag_filter_value:
                        logger.debug(
                            f"Skipping article {raw_article.article_id} because it does not match the tag filter key {tag_filter_key} and value {tag_filter_value}"
                        )
                        continue
                self.candidate_articles.append((raw_article, object_metadata, object_tags))
            return self.candidate_articles
        else:
            raise NotImplementedError(
                f"Result reference type {self.result_ref_type} not implemented"
            )

    def _load_articles_from_s3(
        self, **kwargs: Any
    ) -> list[tuple[str, RawArticle, Mapping[str, str], Mapping[str, str]]]:
        s3_client = kwargs.get("s3_client")
        if not s3_client:
            raise ValueError("s3_client parameter cannot be null")
        publishing_date = kwargs.get("publishing_date")
        if not publishing_date:
            raise ValueError("publishing_date parameter cannot be null")
        publishing_date_str = dt_to_lexicographic_date_s3_prefix(publishing_date)
        prefix = self._get_raw_candidates_s3_object_prefix(publishing_date_str)
        objs_data = read_objects_from_prefix_with_extension(
            CANDIDATE_ARTICLES_S3_BUCKET,
            prefix,
            self.candidate_article_s3_extension,
            s3_client=s3_client,
        )
        return [
            (obj_data[0], RawArticle.parse_raw(obj_data[1]), obj_data[2], obj_data[3])
            for obj_data in objs_data
        ]

    def _get_raw_candidates_s3_object_prefix(self, article_published_date: str) -> str:
        return f"raw_candidate_articles/{article_published_date}/{self.topic_id}"

    # <bucket>/raw_candidate_articles/<article_published_date_str>/<topic_id>/<article_id>.json
    def _get_raw_article_s3_object_key(self, article: RawArticle) -> str:
        date_published = datetime.fromisoformat(article.dt_published)
        article_published_date = dt_to_lexicographic_date_s3_prefix(date_published)
        return f"{self._get_raw_candidates_s3_object_prefix(article_published_date)}/{article.article_id}{self.candidate_article_s3_extension}"

    def store_articles(self, **kwargs: Any) -> tuple[str, list[str]]:
        if self.result_ref_type == ResultRefTypes.S3:
            return self._store_articles_in_s3(**kwargs)
        else:
            raise NotImplementedError(
                f"Result reference type {self.result_ref_type} not implemented"
            )

    def _store_articles_in_s3(self, **kwargs: Any) -> tuple[str, list[str]]:
        s3_client = kwargs.get("s3_client")
        if not s3_client:
            raise ValueError("s3_client parameter cannot be null")
        aggregation_run_id = kwargs.get("aggregation_run_id")
        if not aggregation_run_id:
            raise ValueError("aggregation_run_id parameter cannot be null")
        articles: list[RawArticle] = kwargs["articles"]
        if not all(isinstance(article, RawArticle) for article in articles):
            raise ValueError("articles must be a list of RawArticle")
        prefixes = set()
        for article in articles:
            date_published = datetime.fromisoformat(article.dt_published)
            article_published_date = dt_to_lexicographic_date_s3_prefix(date_published)
            prefix = self._get_raw_candidates_s3_object_prefix(article_published_date)
            prefixes.add(prefix)
            # all stored as json
            object_key = self._get_raw_article_s3_object_key(article)
            body = article.json()
            metadata: Mapping[str, str] = {
                self.aggregation_run_id_metadata_key: aggregation_run_id,
                self.aggregator_id_metadata_key: article.aggregator_id,
            }
            tags: Mapping[str, str] = {
                self.is_sourced_article_tag_key: ARTICLE_NOT_SOURCED_TAGS_FLAG,
            }
            store_object_in_s3(
                CANDIDATE_ARTICLES_S3_BUCKET,
                object_key,
                body,
                object_tags=tags,
                object_metadata=metadata,
                overwrite_allowed=False,
                s3_client=s3_client,
            )
        return CANDIDATE_ARTICLES_S3_BUCKET, list(prefixes)
