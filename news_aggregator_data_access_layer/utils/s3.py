from typing import List, Tuple

import json
from datetime import datetime

import boto3
import botocore

from news_aggregator_data_access_layer.config import REGION_NAME
from news_aggregator_data_access_layer.constants import DT_LEXICOGRAPHIC_STR_FORMAT
from news_aggregator_data_access_layer.exceptions import S3ObjectAlreadyExistsException


def read_objects_from_prefix_with_extension(
    bucket_name: str,
    prefix: str,
    file_extension: str,
    s3_client: boto3.client = boto3.client(
        service_name="s3",
        region_name=REGION_NAME,
    ),
) -> List[Tuple[str, str]]:
    objs_data = []
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
        service_name="s3",
        region_name=REGION_NAME,
    ),
) -> None:
    # check if the object already exists
    try:
        s3_client.head_object(Bucket=bucket_name, Key=object_key)
        print(f"Object {object_key} already exists in S3 bucket {bucket_name}.")
        raise S3ObjectAlreadyExistsException(bucket_name, object_key)
    except botocore.exceptions.ClientError as e:
        # if the object doesn't exist, upload it to S3
        if e.response["Error"]["Code"] == "404":
            print(f"Uploading object {object_key} to S3 bucket {bucket_name}...")
            s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=body)
        else:
            # if there was some other error, raise an exception
            raise e


def dt_to_lexicographic_s3_prefix(dt: datetime) -> str:
    return dt.strftime(DT_LEXICOGRAPHIC_STR_FORMAT)
