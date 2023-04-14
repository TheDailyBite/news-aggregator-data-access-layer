from typing import Any, List, Mapping, Optional, Tuple

import json
from datetime import datetime

from pydantic import BaseModel

from news_aggregator_data_access_layer.config import CANDIDATE_ARTICLES_S3_BUCKET
from news_aggregator_data_access_layer.constants import DT_LEXICOGRAPHIC_STR_FORMAT, ResultRefTypes
from news_aggregator_data_access_layer.utils.s3 import (
    dt_to_lexicographic_s3_prefix,
    read_objects_from_prefix_with_extension,
    store_object_in_s3,
)


class RawArticle(BaseModel):
    article_id: str
    aggregator_id: str
    # this is the search query
    topic: str
    # this is the topic that was discovered by an algo
    discovered_topic: Optional[str]
    category: Optional[str]
    title: str
    url: str
    article_data: str
    # relevance or date
    sorting: str


class CandidateArticles:
    def __init__(self, result_ref_type: ResultRefTypes, candidate_dt: datetime):
        self.result_ref_type = result_ref_type
        self.candidate_dt = candidate_dt
        self.candidate_dt_str = dt_to_lexicographic_s3_prefix(candidate_dt)
        self.candidate_articles: List[RawArticle] = []
        self.candidate_article_s3_extension = ".json"

    def load_articles(self, **kwargs: Any) -> List[RawArticle]:
        if self.result_ref_type == ResultRefTypes.S3:
            unsorted_candidate_articles = self._load_articles_from_s3(**kwargs)
            # TODO - implement sorting
            self.candidate_articles = [a[1] for a in unsorted_candidate_articles]
            return self.candidate_articles
        else:
            raise NotImplementedError(
                f"Result reference type {self.result_ref_type} not implemented"
            )

    # <bucket>/raw_candidate_articles/<candidate_dt_str>/<aggregator_id>/<topic>/<article_id>.json
    def _load_articles_from_s3(self, **kwargs: Any) -> List[Tuple[str, RawArticle]]:
        s3_client = kwargs.get("s3_client")
        topic = kwargs.get("topic")
        if not topic:
            raise ValueError("topic is required")
        aggregator_id = kwargs.get("aggregator_id")
        if not aggregator_id:
            raise ValueError("aggregator_id is required")
        prefix = self._get_raw_candidates_s3_object_prefix(aggregator_id, topic)
        objs_data = read_objects_from_prefix_with_extension(
            CANDIDATE_ARTICLES_S3_BUCKET,
            prefix,
            self.candidate_article_s3_extension,
            s3_client=s3_client,
        )
        return [(obj_data[0], RawArticle.parse_raw(obj_data[1])) for obj_data in objs_data]

    def _get_raw_candidates_s3_object_prefix(self, aggregator_id: str, topic: str) -> str:
        return f"raw_candidate_articles/{self.candidate_dt_str}/{aggregator_id}/{topic}/"

    def _get_raw_article_s3_object_key(self, aggregator_id: str, topic: str, article_id) -> str:
        return f"raw_candidate_articles/{self.candidate_dt_str}/{aggregator_id}/{topic}/{article_id}{self.candidate_article_s3_extension}"

    def store_articles(self, **kwargs: Any) -> Tuple[str, str]:
        if self.result_ref_type == ResultRefTypes.S3:
            return self._store_articles_in_s3(**kwargs)
        else:
            raise NotImplementedError(
                f"Result reference type {self.result_ref_type} not implemented"
            )

    # article id will be <pad_left_0_to_9_digits><index> if sorted by relevance or the published_date + unique_str if sorted by date
    def _store_articles_in_s3(self, **kwargs: Any) -> Tuple[str, str]:
        s3_client = kwargs.get("s3_client")
        topic = kwargs.get("topic")
        if not topic:
            raise ValueError("topic is required")
        aggregator_id = kwargs.get("aggregator_id")
        if not aggregator_id:
            raise ValueError("aggregator_id is required")
        articles: List[RawArticle] = kwargs["articles"]
        if not all(isinstance(article, RawArticle) for article in articles):
            raise ValueError("articles must be a list of RawArticle")
        prefix = self._get_raw_candidates_s3_object_prefix(aggregator_id, topic)
        for article in articles:
            article_id = article.article_id
            # all stored as json
            object_key = self._get_raw_article_s3_object_key(aggregator_id, topic, article_id)
            body = article.json()
            store_object_in_s3(CANDIDATE_ARTICLES_S3_BUCKET, object_key, body, s3_client=s3_client)
        # TODO - store success file?
        return CANDIDATE_ARTICLES_S3_BUCKET, prefix
