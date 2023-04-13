import os

DEPLOYMENT_STAGE = os.environ.get("DEPLOYMENT_STAGE", "dev")
DYNAMODB_HOST = os.environ.get("DYNAMODB_HOST", None)
REGION_NAME = os.environ.get("REGION_NAME", "us-west-1")
DEFAULT_NAMESPACE = os.environ.get("DEFAULT_NAMESPACE", "NewsAggregatorDataAccessLayer")
LOCAL_TESTING = os.environ.get("LOCAL_TESTING", "false").lower() in ["true"]
CANDIDATE_ARTICLES_S3_BUCKET = os.environ.get(
    "CANDIDATE_ARTICLES_S3_BUCKET", f"news-aggregator-candidate-articles-{DEPLOYMENT_STAGE}"
)
