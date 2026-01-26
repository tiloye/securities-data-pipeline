import os
from pathlib import Path
from dotenv import load_dotenv
from prefect_aws import AwsCredentials
from prefect_sqlalchemy import ConnectionComponents, SqlAlchemyConnector, SyncDriver

ENV_NAME = os.getenv("ENV_NAME", "dev")
ENV_PATH = Path(__file__).parent.parent.joinpath(f".env.{ENV_NAME}")
load_dotenv(ENV_PATH)

PREFECT_AWS_KEY_BLOCK = "sec-datalake-credentials"
PREFECT_DW_CONNECTOR_BLOCK = "sec-dw-connector"

if ENV_NAME == "dev":
    # Create s3 and database credentials block in prefect development server

    aws_credentials = AwsCredentials(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
        aws_secret_access_key=os.environ["AWS_SECRET_KEY"]
    )
    aws_credentials.save(PREFECT_AWS_KEY_BLOCK, overwrite=True)

    dw_connector = SqlAlchemyConnector(
        connection_info=ConnectionComponents(
            driver=SyncDriver.POSTGRESQL_PSYCOPG2,
            username=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            host=os.environ["DB_HOST"],
            port=os.environ["DB_PORT"],
            database=os.environ["DB_NAME"],
        )
    )
    dw_connector.save(PREFECT_DW_CONNECTOR_BLOCK, overwrite=True)

# Load s3 credentials
aws_credentials = AwsCredentials.load(PREFECT_AWS_KEY_BLOCK)
AWS_ACCESS_KEY = aws_credentials.aws_access_key_id
AWS_SECRET_KEY = aws_credentials.aws_secret_access_key.get_secret_value()
S3_ENDPOINT = os.environ["S3_ENDPOINT"]
BUCKET_NAME = os.environ["BUCKET_NAME"]
DATA_PATH = f"s3://{BUCKET_NAME}"

# Load database connection
dw_connection = SqlAlchemyConnector.load(PREFECT_DW_CONNECTOR_BLOCK)
DB_ENGINE = dw_connection.get_engine()
DB_HOST = dw_connection.connection_info.host
DB_PORT = dw_connection.connection_info.port
DB_USER = dw_connection.connection_info.username
DB_PASSWORD = dw_connection.connection_info.password.get_secret_value()
DB_NAME = dw_connection.connection_info.database

