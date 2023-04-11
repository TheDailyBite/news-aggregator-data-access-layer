import datetime

import boto3
import botocore.exceptions
import pytest
from moto import mock_s3

from news_aggregator_data_access_layer.exceptions import S3ObjectAlreadyExistsException
from news_aggregator_data_access_layer.utils.s3 import (
    dt_to_lexicographic_s3_prefix,
    read_objects_from_prefix_with_extension,
    store_object_in_s3,
)

TEST_BUCKET_NAME = "test-bucket"


def create_bucket(bucket_name):
    s3 = boto3.resource("s3")
    s3.create_bucket(Bucket=bucket_name)


@mock_s3
def test_store_object_in_s3_success():
    # set the bucket name and object body
    bucket_name = TEST_BUCKET_NAME
    test_key = "test_key"
    object_body = "Hello, world!"
    s3_client = boto3.client("s3")

    create_bucket(bucket_name)
    # test storing a new object
    store_object_in_s3(bucket_name, test_key, object_body, s3_client)
    assert s3_client.head_object(Bucket=bucket_name, Key=test_key)
    assert (
        s3_client.get_object(Bucket=bucket_name, Key=test_key)["Body"].read().decode("utf-8")
        == object_body
    )


@mock_s3
def test_store_object_in_s3_raise_if_exists():
    # set the bucket name and object body
    bucket_name = TEST_BUCKET_NAME
    test_key = "test_key"
    object_body = "Hello, world!"
    s3_client = boto3.client("s3")

    create_bucket(bucket_name)
    # test storing a new object
    store_object_in_s3(bucket_name, test_key, object_body, s3_client)
    assert s3_client.head_object(Bucket=bucket_name, Key=test_key)

    # test storing an object that already exists
    with pytest.raises(S3ObjectAlreadyExistsException) as exc_info:
        store_object_in_s3(bucket_name, test_key, object_body, s3_client)
        assert (
            str(exc_info.value)
            == f"Object {test_key} already exists in S3 bucket {TEST_BUCKET_NAME}."
        )


@mock_s3
def test_read_objects_from_prefix_with_extension():
    # set the bucket name, prefix, and file extension
    bucket_name = TEST_BUCKET_NAME
    prefix = "my-prefix/"
    file_extension = ".txt"

    # create the S3 bucket and upload some test objects
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket=bucket_name)
    s3.put_object(Bucket=bucket_name, Key=prefix + "file1.txt", Body="file1body")
    s3.put_object(Bucket=bucket_name, Key=prefix + "file2.txt", Body="file2body")
    s3.put_object(Bucket=bucket_name, Key=prefix + "file3.csv", Body="file3body")

    # test reading objects with the specified prefix and file extension
    objs_data = read_objects_from_prefix_with_extension(
        bucket_name, prefix, file_extension, s3_client=s3
    )
    assert len(objs_data) == 2
    assert objs_data[0][0] == prefix + "file1.txt"
    assert objs_data[0][1] == "file1body"
    assert objs_data[1][0] == prefix + "file2.txt"
    assert objs_data[1][1] == "file2body"

    # test reading objects with a prefix that doesn't exist
    objs_data = read_objects_from_prefix_with_extension(
        bucket_name, "invalid-prefix/", file_extension, s3_client=s3
    )
    assert len(objs_data) == 0

    # test reading objects with a different file extension
    objs_data = read_objects_from_prefix_with_extension(bucket_name, prefix, ".csv", s3_client=s3)
    assert len(objs_data) == 1
    assert objs_data[0][0] == prefix + "file3.csv"
    assert objs_data[0][1] == "file3body"


def test_dt_to_lexicographic_s3_prefix():
    dt = datetime.datetime(2023, 4, 11, 21, 2, 39, 4166)
    expected_lexicographic_s3_prefix = "2023/04/11/21/02/39/004166"
    assert dt_to_lexicographic_s3_prefix(dt) == expected_lexicographic_s3_prefix