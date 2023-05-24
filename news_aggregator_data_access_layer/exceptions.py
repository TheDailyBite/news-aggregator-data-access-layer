class S3ObjectAlreadyExistsException(Exception):
    """Exception raised when an object already exists in S3"""

    def __init__(self, bucket_name: str, object_key: str):
        self.message = f"Object {object_key} already exists in S3 bucket {bucket_name}."
        super().__init__(self.message)

    def __str__(self):
        return self.message


class S3SuccessFileDoesNotExistException(Exception):
    """Exception raised when a success file does not exist in S3"""

    def __init__(self, bucket_name: str, prefix: str):
        self.message = (
            f"Success file does not exist in S3 bucket {bucket_name} with prefix {prefix}."
        )
        super().__init__(self.message)

    def __str__(self):
        return self.message


class PublishedDateInvalidFormat(Exception):
    """Exception raised when an article has an invalid published date format (i.e. not matching expected for news aggregator)"""

    def __init__(self, published_date: str, expected_published_date_format: str):
        self.message = f"The published date {published_date} does not match expected format {expected_published_date_format}."
        super().__init__(self.message)

    def __str__(self):
        return self.message
