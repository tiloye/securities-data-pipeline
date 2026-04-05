import os
from pathlib import Path
from dotenv import load_dotenv
from prefect_aws import AwsCredentials
from prefect.blocks.system import Secret

ENV_NAME = os.getenv("ENV_NAME", "dev")
ENV_PATH = Path(__file__).parent.parent.joinpath(f".env.{ENV_NAME}")
load_dotenv(ENV_PATH)

PREFECT_AWS_KEY_BLOCK = "sec-datalake-credentials"
PREFECT_DW_CREDENTIALS_BLOCK = "sec-dw-credentials"

if ENV_NAME == "dev":
    # Create s3 and database credentials block in prefect development server

    aws_credentials = AwsCredentials(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
        aws_secret_access_key=os.environ["AWS_SECRET_KEY"],
    )
    aws_credentials.save(PREFECT_AWS_KEY_BLOCK, overwrite=True, _sync=True)

    dw_secret = Secret(
        value={
            "username": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "host": os.getenv("DB_HOST"),
            "port": os.getenv("DB_PORT"),
        }
    )
    dw_secret.save(PREFECT_DW_CREDENTIALS_BLOCK, overwrite=True, _sync=True)

# Load s3 credentials
aws_credentials = AwsCredentials.load(PREFECT_AWS_KEY_BLOCK, _sync=True)
AWS_ACCESS_KEY = aws_credentials.aws_access_key_id
AWS_SECRET_KEY = aws_credentials.aws_secret_access_key.get_secret_value()
S3_ENDPOINT = os.environ["S3_ENDPOINT"]
BUCKET_NAME = os.environ["BUCKET_NAME"]
DATA_PATH = f"s3://{BUCKET_NAME}"

# Load database connection
dw_credentials = Secret.load(PREFECT_DW_CREDENTIALS_BLOCK, _sync=True).get()
DB_TYPE = os.environ["DB_TYPE"]
DB_HOST = dw_credentials.get("host")
DB_PORT = dw_credentials.get("port")
DB_USER = dw_credentials.get("username")
DB_PASSWORD = dw_credentials.get("password")
DB_NAME = os.environ["DB_NAME"]
