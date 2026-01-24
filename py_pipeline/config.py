import os
from pathlib import Path
from dotenv import load_dotenv

ENV_NAME = os.getenv("ENV_NAME", "dev")
ENV_PATH = Path(__file__).parent.parent.joinpath(f".env.{ENV_NAME}")
load_dotenv(ENV_PATH)

AWS_ACCESS_KEY = os.environ["AWS_ACCESS_KEY"]
AWS_SECRET_KEY = os.environ["AWS_SECRET_KEY"]
S3_ENDPOINT = os.environ["S3_ENDPOINT"]
BUCKET_NAME = os.environ["BUCKET_NAME"]
DATA_PATH = f"s3://{BUCKET_NAME}"
DATABASE_URL = os.environ["DATABASE_URL"]
