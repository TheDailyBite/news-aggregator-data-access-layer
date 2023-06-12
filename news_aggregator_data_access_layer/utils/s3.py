from typing import Any, Dict, List, Tuple

import json
import urllib.parse
from collections.abc import Mapping
from datetime import datetime, timezone

import boto3
import botocore

from news_aggregator_data_access_layer.config import REGION_NAME, S3_ENDPOINT_URL
from news_aggregator_data_access_layer.constants import (
    DATE_LEXICOGRAPHIC_STR_FORMAT,
    DT_LEXICOGRAPHIC_DASH_STR_FORMAT,
    DT_LEXICOGRAPHIC_STR_FORMAT,
)
from news_aggregator_data_access_layer.exceptions import (
    S3ObjectAlreadyExistsException,
    S3SuccessFileDoesNotExistException,
)
from news_aggregator_data_access_layer.utils.telemetry import setup_logger

logger = setup_logger(__name__)


def read_objects_from_prefix_with_extension(
    bucket_name: str,
    prefix: str,
    file_extension: str,
    success_marker_fn: str = "_success",
    check_success_file: bool = False,
    s3_client: boto3.client = boto3.client(
        service_name="s3", region_name=REGION_NAME, endpoint_url=S3_ENDPOINT_URL
    ),
) -> list[list[Any]]:
    objs_data = []
    if check_success_file:
        logger.info(
            f"Checking if success file exists at prefix {prefix} with marker fn {success_marker_fn}..."
        )
        if not success_file_exists_at_prefix(bucket_name, prefix, success_marker_fn, s3_client):
            raise S3SuccessFileDoesNotExistException(bucket_name, prefix)
    else:
        logger.info(f"Skipping success file check at prefix {prefix}...")
    logger.info(f"Reading objects from prefix {prefix}...")
    # Objects are returned sorted in an ascending order of the respective key names in the list.
    paginator = s3_client.get_paginator("list_objects_v2")
    for result in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for list_obj in result.get("Contents", []):
            if list_obj["Key"].endswith(file_extension):
                object_key = list_obj["Key"]
                body, metadata, tags = get_object(bucket_name, object_key, s3_client=s3_client)
                objs_data.append([object_key, body, metadata, tags])
    return objs_data


def get_object(
    bucket_name: str,
    object_key: str,
    s3_client: boto3.client = boto3.client(
        service_name="s3", region_name=REGION_NAME, endpoint_url=S3_ENDPOINT_URL
    ),
) -> tuple[str, dict[str, str], dict[str, str]]:
    obj = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    tags = get_object_tags(bucket_name, object_key, s3_client=s3_client)
    return (obj["Body"].read().decode("utf-8"), obj.get("Metadata", dict()), tags)


def get_object_tags(
    bucket_name: str,
    object_key: str,
    s3_client: boto3.client = boto3.client(
        service_name="s3", region_name=REGION_NAME, endpoint_url=S3_ENDPOINT_URL
    ),
) -> dict[str, str]:
    tagging_response = s3_client.get_object_tagging(
        Bucket=bucket_name,
        Key=object_key,
    )
    return create_tagging_map_for_object(tagging_response.get("TagSet", []))


def create_tagging_map_for_object(
    object_tags: list[dict[str, str]],
) -> dict[str, str]:
    return {tag_dict["Key"]: tag_dict["Value"] for tag_dict in object_tags}


def create_tag_set_for_object(
    object_tags: dict[str, str],
) -> list[dict[str, str]]:
    return [{"Key": key, "Value": value} for key, value in object_tags.items()]


def update_object_tags(
    bucket_name: str,
    object_key: str,
    object_tags_to_update: dict[str, str],
    s3_client: boto3.client = boto3.client(
        service_name="s3", region_name=REGION_NAME, endpoint_url=S3_ENDPOINT_URL
    ),
) -> None:
    tagging_to_update = create_tag_set_for_object(object_tags_to_update)
    s3_client.put_object_tagging(
        Bucket=bucket_name,
        Key=object_key,
        Tagging={"TagSet": tagging_to_update},
    )


def store_object_in_s3(
    bucket_name: str,
    object_key: str,
    body: str,
    object_tags: Mapping[str, str] = dict(),
    object_metadata: Mapping[str, str] = dict(),
    overwrite_allowed: bool = False,
    s3_client: boto3.client = boto3.client(
        service_name="s3", region_name=REGION_NAME, endpoint_url=S3_ENDPOINT_URL
    ),
) -> None:
    try:
        if not overwrite_allowed:
            # check if the object already exists
            if object_exists(bucket_name, object_key, s3_client=s3_client):
                raise S3ObjectAlreadyExistsException(bucket_name, object_key)
        encoded_object_tags = urllib.parse.urlencode(object_tags)
        logger.info(
            f"Uploading object {object_key} with tags {encoded_object_tags} and metadata {object_metadata} to S3 bucket {bucket_name} with overwrite allowed value {overwrite_allowed}..."
        )
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=body,
            Metadata=object_metadata,
            Tagging=encoded_object_tags,
        )
    except botocore.exceptions.ClientError as e:
        # if there was some other error, raise an exception
        logger.error(
            f"Error while uploading object {object_key} to S3 bucket {bucket_name}.  Details: {e}",
            exc_info=True,
        )
        raise e


def dt_to_lexicographic_s3_prefix(dt: datetime) -> str:
    return dt.strftime(DT_LEXICOGRAPHIC_STR_FORMAT)


def dt_to_lexicographic_dash_s3_prefix(dt: datetime) -> str:
    return dt.strftime(DT_LEXICOGRAPHIC_DASH_STR_FORMAT)


def lexicographic_s3_prefix_to_dt(prefix: str) -> datetime:
    return datetime.strptime(prefix, DT_LEXICOGRAPHIC_STR_FORMAT)


def dt_to_lexicographic_date_s3_prefix(dt: datetime) -> str:
    return dt.strftime(DATE_LEXICOGRAPHIC_STR_FORMAT)


def store_success_file(
    bucket_name: str,
    prefix: str,
    success_marker_fn: str,
    object_metadata: Mapping[str, str] = dict(),
    s3_client: boto3.client = boto3.client(
        service_name="s3", region_name=REGION_NAME, endpoint_url=S3_ENDPOINT_URL
    ),
) -> None:
    object_key = f"{prefix}/{success_marker_fn}"
    logger.info(f"Uploading success file {object_key} to S3 bucket {bucket_name}...")
    body = dt_to_lexicographic_s3_prefix(datetime.now(timezone.utc))
    store_object_in_s3(
        bucket_name,
        object_key,
        body,
        object_metadata=object_metadata,
        overwrite_allowed=True,
        s3_client=s3_client,
    )


def get_success_file(
    bucket_name: str,
    prefix: str,
    success_marker_fn: str,
    s3_client: boto3.client = boto3.client(
        service_name="s3", region_name=REGION_NAME, endpoint_url=S3_ENDPOINT_URL
    ),
) -> tuple[str, dict[str, str], dict[str, str]]:
    object_key = f"{prefix}/{success_marker_fn}"
    logger.info(f"Downloading success file {object_key} from S3 bucket {bucket_name}...")
    return get_object(bucket_name, object_key, s3_client=s3_client)


def object_exists(
    bucket_name: str,
    object_key: str,
    s3_client: boto3.client = boto3.client(
        service_name="s3", region_name=REGION_NAME, endpoint_url=S3_ENDPOINT_URL
    ),
) -> bool:
    try:
        s3_client.head_object(Bucket=bucket_name, Key=object_key)
        return True
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            # if there was some other error, raise an exception
            logger.error(
                f"Error while checking if object {object_key} exists in S3 bucket {bucket_name}. Details: {e}",
                exc_info=True,
            )
            raise e


def success_file_exists_at_prefix(
    bucket_name: str,
    prefix: str,
    success_marker_fn: str,
    s3_client: boto3.client = boto3.client(
        service_name="s3", region_name=REGION_NAME, endpoint_url=S3_ENDPOINT_URL
    ),
) -> bool:
    object_key = f"{prefix}/{success_marker_fn}"
    return object_exists(bucket_name, object_key, s3_client=s3_client)
