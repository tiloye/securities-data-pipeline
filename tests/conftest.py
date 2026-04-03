import pytest
from minio import Minio
from sqlalchemy import create_engine, text

from py_pipeline.config import (
    AWS_ACCESS_KEY,
    AWS_SECRET_KEY,
    BUCKET_NAME,
    S3_ENDPOINT,
    DB_HOST,
    DB_PORT,
    DB_USER,
    DB_PASSWORD,
    DB_NAME,
)

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

client = Minio(
    S3_ENDPOINT.replace("http://", ""),
    access_key=AWS_ACCESS_KEY,
    secret_key=AWS_SECRET_KEY,
    secure=False,
)


@pytest.fixture(autouse=True, scope="session")
def s3_bucket():
    client.make_bucket(BUCKET_NAME)
    yield
    client.remove_bucket(BUCKET_NAME)


@pytest.fixture
def remove_s3_objects():
    yield
    objs = [obj.object_name for obj in client.list_objects(BUCKET_NAME, recursive=True)]
    for obj in objs:
        client.remove_object(BUCKET_NAME, obj)


@pytest.fixture
def drop_dw_tables():
    yield
    with engine.connect() as con:
        for asset_category in ("fx", "sp_stocks"):
            con.execute(text(f"DROP TABLE IF EXISTS symbols_{asset_category};"))
            con.execute(text(f"DROP TABLE IF EXISTS price_history_{asset_category};"))
        for table in ["_dlt_loads", "_dlt_pipeline_state", "_dlt_version"]:
            con.execute(text(f"DROP TABLE IF EXISTS {table};"))
        con.commit()
