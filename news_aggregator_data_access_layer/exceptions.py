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
