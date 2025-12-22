import os
from dotenv import load_dotenv

load_dotenv()

ENV_NAME = os.getenv("ENV_NAME", "dev")

AWS_ACCESS_KEY = os.environ["AWS_ACCESS_KEY"]
AWS_SECRET_KEY = os.environ["AWS_SECRET_KEY"]
S3_ENDPOINT = os.environ["S3_ENDPOINT"]
BUCKET_NAME = os.environ["BUCKET_NAME"]
DATABASE_URL = os.environ["DATABASE_URL"]

if ENV_NAME == "dev":
    BUCKET_NAME = "test-" + BUCKET_NAME

DATA_PATH = f"s3://{BUCKET_NAME}"