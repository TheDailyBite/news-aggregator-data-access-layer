from typing import Any, List, Optional, Tuple

import copy
import json
from collections.abc import Mapping
from datetime import datetime
from enum import Enum

import tldextract
from newsplease import NewsPlease
from pydantic import BaseModel, Field

from news_aggregator_data_access_layer.config import CANDIDATE_ARTICLES_S3_BUCKET
from news_aggregator_data_access_layer.constants import (
    ARTICLE_NOT_SOURCED_TAGS_FLAG,
    ARTICLE_SOURCED_TAGS_FLAG,
    DATE_PUBLISHED_ARTICLE_REGEX,
    DT_LEXICOGRAPHIC_STR_FORMAT,
    NO_CATEGORY_STR,
    ArticleType,
    ResultRefTypes,
)
from news_aggregator_data_access_layer.utils.s3 import (
    dt_to_lexicographic_date_s3_prefix,
    dt_to_lexicographic_s3_prefix,
    get_object_tags,
    get_success_file,
    read_objects_from_prefix_with_extension,
    store_object_in_s3,
    store_success_file,
    success_file_exists_at_prefix,
    update_object_tags,
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
    category: Optional[str] = NO_CATEGORY_STR
    title: str
    url: str
    author: Optional[str] = ""
    article_full_text: Optional[str] = ""
    article_text_snippet: Optional[str] = ""
    # the meta description included in many articles
    article_text_description: Optional[str] = ""
    # this is the json string of the Article representation of the aggregator used
    article_data: str
    # one of SUPPORTED_SORTING strings
    sorting: str
    article_type: str = ArticleType.NEWS.value
    provider_domain: Optional[str] = ""
    article_processed_data: Optional[str] = ""

    def process_article_data(self):
        if self.article_processed_data:
            return self.article_processed_data
        else:
            # TODO - try newspaper3k
            article = NewsPlease.from_url(self.url)
            if not self.provider_domain:
                ext_res = tldextract.extract(self.url)
                parts = []
                if ext_res.subdomain:
                    if ext_res.subdomain.lower() != "www":
                        parts.append(ext_res.subdomain.lower())
                if ext_res.domain:
                    parts.append(ext_res.domain.lower())
                if ext_res.suffix:
                    parts.append(ext_res.suffix.lower())
                self.provider_domain = ".".join(parts)
            # NOTE - some articles return 200 but have no maintext so we skip them
            if not article or not article.maintext:
                logger.warning(
                    f"Could not process article with url {self.url} and provider domain {self.provider_domain}"
                )
                # TODO - emit metric with domain
                return
            self.article_full_text = article.maintext
            if not self.article_text_description:
                self.article_text_description = article.description
            article_processed_data_dict = article.get_serializable_dict().pop("maintext")
            self.article_processed_data = json.dumps(article_processed_data_dict)

    def get_article_text(self) -> str:
        if not self.article_full_text:
            self.process_article_data()
        return self.article_full_text

    def get_article_text_description(self) -> str:
        if not self.article_text_description:
            self.process_article_data()
        return self.article_text_description


class RawArticleEmbedding(BaseModel):
    article_id: str
    embedding_type: str
    embedding_model_name: str
    embedding: list[float]


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
        self,
        tag_filter_key: Optional[str] = "",
        tag_filter_value: Optional[str] = "",
        **kwargs: Any,
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
            unique_urls = set()
            for a in unsorted_candidate_articles:
                raw_article = a[1]
                object_metadata = a[2]
                object_tags = a[3]
                # filter to only exclude non-matching articles
                if tag_filter_key and tag_filter_value:
                    if object_tags[tag_filter_key] != tag_filter_value:
                        logger.warning(
                            f"Skipping article {raw_article.article_id} because it does not match the tag filter key {tag_filter_key} and value {tag_filter_value}"
                        )
                        continue
                # NOTE - filter to only include unique articles which are determined via URL currently
                if raw_article.url in unique_urls:
                    logger.warning(
                        f"Skipping article {raw_article.article_id} because it is a duplicate of another article with the same url"
                    )
                    continue
                unique_urls.add(raw_article.url)
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
        return f"raw_candidate_articles/{self.topic_id}/{article_published_date}"

    def _get_raw_candidate_embeddings_s3_object_prefix(self, article_published_date: str) -> str:
        return f"raw_candidate_article_embeddings/{self.topic_id}/{article_published_date}"

    # <bucket>/raw_candidate_articles/<topic_id>/<article_published_date_str>/<article_id>.json
    def _get_raw_article_s3_object_key(self, article: RawArticle) -> str:
        date_published = datetime.fromisoformat(article.dt_published)
        article_published_date = dt_to_lexicographic_date_s3_prefix(date_published)
        return f"{self._get_raw_candidates_s3_object_prefix(article_published_date)}/{article.article_id}{self.candidate_article_s3_extension}"

    # <bucket>/raw_candidate_articles/<topic_id>/<article_published_date_str>/embeddings/<article_id>.json
    def _get_raw_article_embedding_s3_object_key(self, article: RawArticle) -> str:
        date_published = datetime.fromisoformat(article.dt_published)
        article_published_date = dt_to_lexicographic_date_s3_prefix(date_published)
        return f"{self._get_raw_candidate_embeddings_s3_object_prefix(article_published_date)}/{article.article_id}{self.candidate_article_s3_extension}"

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

    def store_embeddings(self, **kwargs: Any) -> tuple[str, list[str]]:
        if self.result_ref_type == ResultRefTypes.S3:
            return self._store_embeddings_in_s3(**kwargs)
        else:
            raise NotImplementedError(
                f"Result reference type {self.result_ref_type} not implemented"
            )

    def _store_embeddings_in_s3(self, **kwargs: Any) -> tuple[str, list[str]]:
        s3_client = kwargs.get("s3_client")
        if not s3_client:
            raise ValueError("s3_client parameter cannot be null")
        articles: list[RawArticle] = kwargs["articles"]
        if not all(isinstance(article, RawArticle) for article in articles):
            raise ValueError("articles must be a list of RawArticle")
        embeddings: list[RawArticleEmbedding] = kwargs["embeddings"]
        if not all(isinstance(embedding, RawArticleEmbedding) for embedding in embeddings):
            raise ValueError("embeddings must be a list of RawArticleEmbedding")
        prefixes = set()
        for article, embedding in zip(articles, embeddings):
            if article.article_id != embedding.article_id:
                raise ValueError(
                    "article_id in article and embedding not matching.Articles and embeddings must be aligned"
                )
            date_published = datetime.fromisoformat(article.dt_published)
            article_published_date = dt_to_lexicographic_date_s3_prefix(date_published)
            prefix = self._get_raw_candidate_embeddings_s3_object_prefix(article_published_date)
            prefixes.add(prefix)
            # all stored as json
            object_key = self._get_raw_article_embedding_s3_object_key(article)
            body = embedding.json()
            store_object_in_s3(
                CANDIDATE_ARTICLES_S3_BUCKET,
                object_key,
                body,
                overwrite_allowed=True,
                s3_client=s3_client,
            )
        return CANDIDATE_ARTICLES_S3_BUCKET, list(prefixes)

    def update_articles_is_sourced_tag(self, **kwargs: Any) -> None:
        if self.result_ref_type == ResultRefTypes.S3:
            return self._update_s3_articles_is_sourced_tag(**kwargs)
        else:
            raise NotImplementedError(
                f"Result reference type {self.result_ref_type} not implemented"
            )

    def _update_s3_articles_is_sourced_tag(self, **kwargs: Any) -> None:
        s3_client = kwargs.get("s3_client")
        if not s3_client:
            raise ValueError("s3_client parameter cannot be null")
        articles: list[RawArticle] = kwargs["articles"]
        if not all(isinstance(article, RawArticle) for article in articles):
            raise ValueError("articles must be a list of RawArticle")
        updated_tag_value = kwargs["updated_tag_value"]
        if updated_tag_value not in [ARTICLE_SOURCED_TAGS_FLAG, ARTICLE_NOT_SOURCED_TAGS_FLAG]:
            raise ValueError(
                f"updated_tag_value must be one of {ARTICLE_SOURCED_TAGS_FLAG} or {ARTICLE_NOT_SOURCED_TAGS_FLAG}"
            )
        for article in articles:
            object_key = self._get_raw_article_s3_object_key(article)
            existing_tags = get_object_tags(
                bucket_name=CANDIDATE_ARTICLES_S3_BUCKET,
                object_key=object_key,
                s3_client=s3_client,
            )
            tags_to_update = {
                self.is_sourced_article_tag_key: updated_tag_value,
            }
            existing_tags.update(tags_to_update)
            logger.info(
                f"Updating tags for {object_key} to {existing_tags} which will update the is_sourced_article tag to {updated_tag_value}"
            )
            update_object_tags(
                bucket_name=CANDIDATE_ARTICLES_S3_BUCKET,
                object_key=object_key,
                object_tags_to_update=existing_tags,
                s3_client=s3_client,
            )
