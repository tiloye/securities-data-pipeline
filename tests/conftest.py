import pytest
from minio import Minio
from prefect.testing.utilities import prefect_test_harness
from sqlalchemy import text

from py_pipeline.config import (
    AWS_ACCESS_KEY,
    AWS_SECRET_KEY,
    BUCKET_NAME,
    DB_ENGINE,
    S3_ENDPOINT,
)

@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    with prefect_test_harness():
        yield

engine = DB_ENGINE
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
        con.commit()
