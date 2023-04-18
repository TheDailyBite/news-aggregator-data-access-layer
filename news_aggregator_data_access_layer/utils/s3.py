from typing import Any, List

import json
from datetime import datetime

import boto3
import botocore

from news_aggregator_data_access_layer.config import REGION_NAME, S3_ENDPOINT_URL
from news_aggregator_data_access_layer.constants import DT_LEXICOGRAPHIC_STR_FORMAT
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
    success_marker_fn: str,
    s3_client: boto3.client = boto3.client(
        service_name="s3", region_name=REGION_NAME, endpoint_url=S3_ENDPOINT_URL
    ),
) -> List[List[Any]]:
    objs_data = []
    if not success_file_exists_at_prefix(bucket_name, prefix, success_marker_fn, s3_client):
        raise S3SuccessFileDoesNotExistException(bucket_name, prefix)
    # Objects are returned sorted in an ascending order of the respective key names in the list.
    paginator = s3_client.get_paginator("list_objects_v2")
    for result in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in result.get("Contents", []):
            if obj["Key"].endswith(file_extension):
                object_key = obj["Key"]
                body = (
                    s3_client.get_object(Bucket=bucket_name, Key=obj["Key"])["Body"]
                    .read()
                    .decode("utf-8")
                )
                objs_data.append([object_key, body])
    return objs_data


def store_object_in_s3(
    bucket_name: str,
    object_key: str,
    body: str,
    s3_client: boto3.client = boto3.client(
        service_name="s3", region_name=REGION_NAME, endpoint_url=S3_ENDPOINT_URL
    ),
) -> None:
    # check if the object already exists
    try:
        s3_client.head_object(Bucket=bucket_name, Key=object_key)
        logger.error(f"Object {object_key} already exists in S3 bucket {bucket_name}.")
        raise S3ObjectAlreadyExistsException(bucket_name, object_key)
    except botocore.exceptions.ClientError as e:
        # if the object doesn't exist, upload it to S3
        if e.response["Error"]["Code"] == "404":
            logger.info(f"Uploading object {object_key} to S3 bucket {bucket_name}...")
            s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=body)
        else:
            # if there was some other error, raise an exception
            logger.error(
                f"Error while uploading object {object_key} to S3 bucket {bucket_name}.  Details: {e}",
                exc_info=True,
            )
            raise e


def dt_to_lexicographic_s3_prefix(dt: datetime) -> str:
    return dt.strftime(DT_LEXICOGRAPHIC_STR_FORMAT)


def store_success_file(
    bucket_name: str,
    prefix: str,
    success_marker_fn: str,
    s3_client: boto3.client = boto3.client(
        service_name="s3", region_name=REGION_NAME, endpoint_url=S3_ENDPOINT_URL
    ),
) -> None:
    object_key = f"{prefix}/{success_marker_fn}"
    logger.info(f"Uploading success file {object_key} to S3 bucket {bucket_name}...")
    body = dt_to_lexicographic_s3_prefix(datetime.utcnow())
    store_object_in_s3(bucket_name, object_key, body, s3_client=s3_client)


def success_file_exists_at_prefix(
    bucket_name: str,
    prefix: str,
    success_marker_fn: str,
    s3_client: boto3.client = boto3.client(
        service_name="s3", region_name=REGION_NAME, endpoint_url=S3_ENDPOINT_URL
    ),
) -> bool:
    try:
        object_key = f"{prefix}/{success_marker_fn}"
        s3_client.head_object(Bucket=bucket_name, Key=object_key)
        return True
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            # if there was some other error, raise an exception
            logger.error(
                f"Error while checking if success file exists at prefix {prefix} in S3 bucket {bucket_name}. Details: {e}",
                exc_info=True,
            )
            raise e
