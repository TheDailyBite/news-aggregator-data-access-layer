import os

DEPLOYMENT_STAGE = os.environ.get("DEPLOYMENT_STAGE", "dev")
DYNAMODB_HOST = os.environ.get("DYNAMODB_HOST", None)
REGION_NAME = os.environ.get("REGION_NAME", "us-west-1")
DEFAULT_NAMESPACE = os.environ.get("DEFAULT_NAMESPACE", "NewsAggregatorDataAccessLayer")
LOCAL_TESTING = os.environ.get("LOCAL_TESTING", "false").lower() in ["true"]
CANDIDATE_ARTICLES_S3_BUCKET = os.environ.get(
    "CANDIDATE_ARTICLES_S3_BUCKET", f"news-aggregator-candidate-articles-{DEPLOYMENT_STAGE}"
)
SOURCED_ARTICLES_S3_BUCKET = os.environ.get(
    "SOURCED_ARTICLES_S3_BUCKET", f"news-aggregator-sourced-articles-{DEPLOYMENT_STAGE}"
)
SELF_USER_ID = "___bamchip___"
DEFAULT_LOGGER_NAME = "news_aggregator_data_access_layer"
S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", f"https://s3.{REGION_NAME}.amazonaws.com")
