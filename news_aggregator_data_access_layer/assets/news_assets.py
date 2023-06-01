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
    date_published: str = Field(regex=DATE_PUBLISHED_ARTICLE_REGEX)
    aggregation_index: int
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
    article_processed_data: Optional[str] = ""

    def process_article_data(self):
        if self.article_processed_data:
            return self.article_processed_data
        else:
            article = NewsPlease.from_url(self.url)
            if not article:
                ext_res = tldextract.extract(self.url)
                domain = ext_res.domain.lower()
                logger.warning(f"Could not process article with url {self.url} and domain {domain}")
                # TODO - emit metric with domain
                return
            self.article_processed_data = json.dumps(article.get_serializable_dict())


class CandidateArticles:
    def __init__(
        self,
        result_ref_type: ResultRefTypes,
        aggregator_id: str,
        aggregation_run_id: str,
        aggregation_dt: datetime,
    ):
        self.result_ref_type = result_ref_type
        self.aggregation_dt = aggregation_dt
        self.aggregation_date_str = dt_to_lexicographic_date_s3_prefix(aggregation_dt)
        self.aggregator_id = aggregator_id
        self.aggregation_run_id = aggregation_run_id
        self.candidate_articles: list[RawArticle] = []
        self.candidate_article_s3_extension = ".json"
        self.success_marker_fn = "__SUCCESS__"
        self.success_metadata_aggregators_key = "aggregators"
        self.success_metadata_aggregators_dt_key = "aggregators_dt"
        self.default_success_file_metadata = {
            self.success_metadata_aggregators_key: "",
            self.success_metadata_aggregators_dt_key: "",
        }

    def load_articles(self, **kwargs: Any) -> list[RawArticle]:
        if self.result_ref_type == ResultRefTypes.S3:
            unsorted_candidate_articles = self._load_articles_from_s3(**kwargs)
            # TODO - implement sorting
            self.candidate_articles = [a[1] for a in unsorted_candidate_articles]
            return self.candidate_articles
        else:
            raise NotImplementedError(
                f"Result reference type {self.result_ref_type} not implemented"
            )

    # <bucket>/raw_candidate_articles/<aggregation_run_id>/<article_id>.json
    def _load_articles_from_s3(self, **kwargs: Any) -> list[tuple[str, RawArticle]]:
        s3_client = kwargs.get("s3_client")
        if not s3_client:
            raise ValueError("s3_client parameter cannot be null")
        prefix = self._get_raw_candidates_s3_object_prefix()
        objs_data = read_objects_from_prefix_with_extension(
            CANDIDATE_ARTICLES_S3_BUCKET,
            prefix,
            self.candidate_article_s3_extension,
            self.success_marker_fn,
            check_success_file=True,
            s3_client=s3_client,
        )
        success_file_body, metadata = get_success_file(
            CANDIDATE_ARTICLES_S3_BUCKET, prefix, self.success_marker_fn, s3_client=s3_client
        )
        logger.info(f"Success file body: {success_file_body} and metadata {metadata}")
        return [(obj_data[0], RawArticle.parse_raw(obj_data[1])) for obj_data in objs_data]

    def _get_raw_candidates_s3_object_prefix(self) -> str:
        return f"raw_candidate_articles/{self.aggregation_run_id}"

    def _get_raw_article_s3_object_key(self, article_id: str) -> str:
        return f"{self._get_raw_candidates_s3_object_prefix()}/{article_id}{self.candidate_article_s3_extension}"

    def store_articles(self, **kwargs: Any) -> tuple[str, str]:
        if self.result_ref_type == ResultRefTypes.S3:
            return self._store_articles_in_s3(**kwargs)
        else:
            raise NotImplementedError(
                f"Result reference type {self.result_ref_type} not implemented"
            )

    def _store_articles_in_s3(self, **kwargs: Any) -> tuple[str, str]:
        s3_client = kwargs.get("s3_client")
        if not s3_client:
            raise ValueError("s3_client parameter cannot be null")
        articles: list[RawArticle] = kwargs["articles"]
        if not all(isinstance(article, RawArticle) for article in articles):
            raise ValueError("articles must be a list of RawArticle")
        prefix = self._get_raw_candidates_s3_object_prefix()
        for article in articles:
            article_id = article.article_id
            # all stored as json
            object_key = self._get_raw_article_s3_object_key(article_id)
            body = article.json()
            metadata: Mapping[str, str] = dict()
            store_object_in_s3(
                CANDIDATE_ARTICLES_S3_BUCKET,
                object_key,
                body,
                object_metadata=metadata,
                overwrite_allowed=False,
                s3_client=s3_client,
            )
        success_obj_metadata = copy.deepcopy(self.default_success_file_metadata)
        aggregation_dt_str = dt_to_lexicographic_s3_prefix(self.aggregation_dt)
        success_obj_metadata[self.success_metadata_aggregators_key] = self.aggregator_id
        success_obj_metadata[self.success_metadata_aggregators_dt_key] = aggregation_dt_str
        store_success_file(
            CANDIDATE_ARTICLES_S3_BUCKET,
            prefix,
            self.success_marker_fn,
            object_metadata=success_obj_metadata,
            s3_client=s3_client,
        )
        return CANDIDATE_ARTICLES_S3_BUCKET, prefix
