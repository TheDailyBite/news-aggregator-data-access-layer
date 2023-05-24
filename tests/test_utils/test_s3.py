import datetime
import re

import boto3
import botocore.exceptions
import pytest
from moto import mock_s3

from news_aggregator_data_access_layer.constants import DT_LEXICOGRAPHIC_STR_REGEX
from news_aggregator_data_access_layer.exceptions import (
    S3ObjectAlreadyExistsException,
    S3SuccessFileDoesNotExistException,
)
from news_aggregator_data_access_layer.utils.s3 import (
    dt_to_lexicographic_date_s3_prefix,
    dt_to_lexicographic_s3_prefix,
    get_object,
    get_success_file,
    lexicographic_s3_prefix_to_dt,
    read_objects_from_prefix_with_extension,
    store_object_in_s3,
    store_success_file,
    success_file_exists_at_prefix,
)

TEST_BUCKET_NAME = "test-bucket"


def create_bucket(bucket_name):
    s3 = boto3.resource("s3")
    s3.create_bucket(Bucket=bucket_name)


@mock_s3
def test_store_object_in_s3_success_without_metadata():
    # set the bucket name and object body
    bucket_name = TEST_BUCKET_NAME
    test_key = "test_key"
    object_body = "Hello, world!"
    s3_client = boto3.client("s3")

    create_bucket(bucket_name)
    # test storing a new object
    store_object_in_s3(bucket_name, test_key, object_body, s3_client=s3_client)
    assert s3_client.head_object(Bucket=bucket_name, Key=test_key)
    assert (
        s3_client.get_object(Bucket=bucket_name, Key=test_key)["Body"].read().decode("utf-8")
        == object_body
    )


@mock_s3
def test_store_object_in_s3_success_with_metadata():
    # set the bucket name and object body
    bucket_name = TEST_BUCKET_NAME
    test_key = "test_key"
    object_body = "Hello, world!"
    s3_client = boto3.client("s3")
    metadata = {"test_key": "test_value"}

    create_bucket(bucket_name)
    # test storing a new object
    store_object_in_s3(
        bucket_name, test_key, object_body, object_metadata=metadata, s3_client=s3_client
    )
    assert s3_client.head_object(Bucket=bucket_name, Key=test_key)
    s3_obj = s3_client.get_object(Bucket=bucket_name, Key=test_key)
    assert s3_obj["Body"].read().decode("utf-8") == object_body
    assert s3_obj["Metadata"] == metadata


@mock_s3
def test_store_object_in_s3_success_without_metadata_overwrite_allowed():
    # set the bucket name and object body
    bucket_name = TEST_BUCKET_NAME
    test_key = "test_key"
    object_body = "Hello, world!"
    object_body_overwrite = "Hello, world! Overwrite!"
    s3_client = boto3.client("s3")

    create_bucket(bucket_name)
    # test storing a new object
    store_object_in_s3(bucket_name, test_key, object_body, s3_client=s3_client)
    store_object_in_s3(
        bucket_name, test_key, object_body_overwrite, overwrite_allowed=True, s3_client=s3_client
    )
    assert s3_client.head_object(Bucket=bucket_name, Key=test_key)
    assert (
        s3_client.get_object(Bucket=bucket_name, Key=test_key)["Body"].read().decode("utf-8")
        == object_body_overwrite
    )


@mock_s3
def test_store_object_in_s3_raise_if_exists_overwrite_not_allowed():
    # set the bucket name and object body
    bucket_name = TEST_BUCKET_NAME
    test_key = "test_key"
    object_body = "Hello, world!"
    s3_client = boto3.client("s3")

    create_bucket(bucket_name)
    # test storing a new object
    store_object_in_s3(bucket_name, test_key, object_body, s3_client=s3_client)
    assert s3_client.head_object(Bucket=bucket_name, Key=test_key)

    # test storing an object that already exists
    with pytest.raises(S3ObjectAlreadyExistsException) as exc_info:
        store_object_in_s3(
            bucket_name, test_key, object_body, overwrite_allowed=False, s3_client=s3_client
        )
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
    success_marker_fn = "__SUCCESS__"
    test_metadata_csv = {"some-key": "some-value"}

    # create the S3 bucket and upload some test objects
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket=bucket_name)
    s3.put_object(Bucket=bucket_name, Key=prefix + "file1.txt", Body="file1body")
    s3.put_object(Bucket=bucket_name, Key=prefix + "file2.txt", Body="file2body")
    s3.put_object(
        Bucket=bucket_name, Key=prefix + "file3.csv", Body="file3body", Metadata=test_metadata_csv
    )
    store_success_file(bucket_name, prefix, success_marker_fn, s3_client=s3)

    # test reading objects with the specified prefix and file extension
    objs_data = read_objects_from_prefix_with_extension(
        bucket_name, prefix, file_extension, success_marker_fn, s3_client=s3
    )
    assert len(objs_data) == 2
    assert objs_data[0][0] == prefix + "file1.txt"
    assert objs_data[0][1] == "file1body"
    assert objs_data[0][2] == dict()
    assert objs_data[1][0] == prefix + "file2.txt"
    assert objs_data[1][1] == "file2body"
    assert objs_data[0][2] == dict()

    # test reading objects with a different file extension
    objs_data = read_objects_from_prefix_with_extension(
        bucket_name, prefix, ".csv", success_marker_fn, s3_client=s3
    )
    assert len(objs_data) == 1
    assert objs_data[0][0] == prefix + "file3.csv"
    assert objs_data[0][1] == "file3body"
    assert objs_data[0][2] == test_metadata_csv


@mock_s3
def test_read_objects_from_prefix_with_extension_without_successfile_check():
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
    assert objs_data[0][2] == dict()
    assert objs_data[1][0] == prefix + "file2.txt"
    assert objs_data[1][1] == "file2body"
    assert objs_data[1][2] == dict()


@mock_s3
def test_read_objects_from_prefix_with_extension_raise_due_to_no_success_file():
    # set the bucket name, prefix, and file extension
    bucket_name = TEST_BUCKET_NAME
    prefix = "my-prefix/"
    file_extension = ".txt"
    success_marker_fn = "__SUCCESS__"

    # create the S3 bucket and upload some test objects
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket=bucket_name)
    s3.put_object(Bucket=bucket_name, Key=prefix + "file1.txt", Body="file1body")
    s3.put_object(Bucket=bucket_name, Key=prefix + "file2.txt", Body="file2body")
    s3.put_object(Bucket=bucket_name, Key=prefix + "file3.csv", Body="file3body")

    with pytest.raises(S3SuccessFileDoesNotExistException) as exc_info:
        # test reading objects with the specified prefix and file extension
        objs_data = read_objects_from_prefix_with_extension(
            bucket_name,
            prefix,
            file_extension,
            success_marker_fn,
            check_success_file=True,
            s3_client=s3,
        )
        assert (
            exc_info.value.message
            == f"Success file does not exist in S3 bucket {bucket_name} with prefix {prefix}."
        )


@mock_s3
def test_get_object():
    # set the bucket name, prefix, and file extension
    bucket_name = TEST_BUCKET_NAME
    key = "my-key.csv"
    test_metadata_csv = {"some-key": "some-value"}

    # create the S3 bucket and upload some test objects
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket=bucket_name)
    s3.put_object(Bucket=bucket_name, Key=key, Body="file1body", Metadata=test_metadata_csv)

    # test reading objects with the specified prefix and file extension
    obj_data = get_object(bucket_name, key, s3_client=s3)
    assert obj_data[0] == "file1body"
    assert obj_data[1] == test_metadata_csv


@mock_s3
def test_store_success_file_without_metadata():
    # set the bucket name, prefix, and file extension
    bucket_name = TEST_BUCKET_NAME
    prefix = "my-prefix"
    success_marker_fn = "__SUCCESS__"

    # create the S3 bucket and upload some test objects
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket=bucket_name)

    # test storing a success file
    store_success_file(bucket_name, prefix, success_marker_fn, s3_client=s3)
    body, metadata = get_success_file(bucket_name, prefix, success_marker_fn, s3_client=s3)
    assert not metadata


@mock_s3
def test_store_success_file_with_metadata():
    # set the bucket name, prefix, and file extension
    bucket_name = TEST_BUCKET_NAME
    prefix = "my-prefix"
    success_marker_fn = "__SUCCESS__"
    metadata = {"some-key": "some-value"}

    # create the S3 bucket and upload some test objects
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket=bucket_name)

    # test storing a success file
    store_success_file(
        bucket_name, prefix, success_marker_fn, object_metadata=metadata, s3_client=s3
    )
    body, metadata = get_success_file(bucket_name, prefix, success_marker_fn, s3_client=s3)
    assert metadata == {"some-key": "some-value"}


@mock_s3
def test_get_success_file_without_metadata():
    # set the bucket name, prefix, and file extension
    bucket_name = TEST_BUCKET_NAME
    prefix = "my-prefix"
    success_marker_fn = "__SUCCESS__"

    # create the S3 bucket and upload some test objects
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket=bucket_name)
    store_success_file(bucket_name, prefix, success_marker_fn, s3_client=s3)

    # test getting the success file
    body, metadata = get_success_file(bucket_name, prefix, success_marker_fn, s3_client=s3)
    assert re.match(DT_LEXICOGRAPHIC_STR_REGEX, body)
    assert not metadata


@mock_s3
def test_get_success_file_with_metadata():
    # set the bucket name, prefix, and file extension
    bucket_name = TEST_BUCKET_NAME
    prefix = "my-prefix"
    success_marker_fn = "__SUCCESS__"
    metadata = {"aggregators": "bing,newsapi"}

    # create the S3 bucket and upload some test objects
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket=bucket_name)
    store_success_file(
        bucket_name, prefix, success_marker_fn, object_metadata=metadata, s3_client=s3
    )

    # test getting the success file
    body, actual_metadata = get_success_file(bucket_name, prefix, success_marker_fn, s3_client=s3)
    assert re.match(DT_LEXICOGRAPHIC_STR_REGEX, body)
    assert actual_metadata == metadata


@mock_s3
def test_success_file_exists_at_prefix():
    # set the bucket name, prefix, and file extension
    bucket_name = TEST_BUCKET_NAME
    prefix = "my-prefix"
    success_marker_fn = "__SUCCESS__"

    # create the S3 bucket and upload some test objects
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket=bucket_name)
    store_success_file(bucket_name, prefix, success_marker_fn, s3_client=s3)

    # test success file exists
    expected_result = True
    actual_result = success_file_exists_at_prefix(
        bucket_name, prefix, success_marker_fn, s3_client=s3
    )
    assert actual_result == expected_result


@mock_s3
def test_success_file_not_exists_at_prefix():
    # set the bucket name, prefix, and file extension
    bucket_name = TEST_BUCKET_NAME
    prefix = "my-prefix"
    success_marker_fn = "__SUCCESS__"

    # create the S3 bucket and upload some test objects
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket=bucket_name)

    # test success file exists
    expected_result = False
    actual_result = success_file_exists_at_prefix(
        bucket_name, prefix, success_marker_fn, s3_client=s3
    )
    assert actual_result == expected_result


def test_dt_to_lexicographic_s3_prefix():
    dt = datetime.datetime(2023, 4, 11, 21, 2, 39, 4166)
    expected_lexicographic_s3_prefix = "2023/04/11/21/02/39/004166"
    assert dt_to_lexicographic_s3_prefix(dt) == expected_lexicographic_s3_prefix


def test_lexicographic_s3_prefix_to_dt():
    lexicographic_s3_prefix = "2023/04/11/21/02/39/004166"
    expected_dt = datetime.datetime(2023, 4, 11, 21, 2, 39, 4166)
    assert lexicographic_s3_prefix_to_dt(lexicographic_s3_prefix) == expected_dt


def test_dt_to_lexicographic_date_s3_prefix():
    dt = datetime.datetime(2023, 4, 11, 21, 2, 39, 4166)
    expected_lexicographic_date_s3_prefix = "2023/04/11"
    assert dt_to_lexicographic_date_s3_prefix(dt) == expected_lexicographic_date_s3_prefix
