from dotenv import dotenv_values
from prefect import deploy
from prefect.runner.storage import LocalStorage

from .main import etl

if __name__ == "__main__":
    ENV_VARS = dotenv_values()

    fx_flow_deployment = etl.to_deployment(
        "fx-data-pipeline",
        cron="0 0 * * 1-5",
        parameters={"asset_category": "fx"},
        job_variables={"env": ENV_VARS},
    )
    fx_flow_deployment.storage = LocalStorage(path="/app")

    sp_stocks_flow_deployment = etl.to_deployment(
        "sp-stocks-data-pipeline",
        cron="0 0 * * 1-5",
        parameters={"asset_category": "sp_stocks"},
        job_variables={"env": ENV_VARS},
    )
    sp_stocks_flow_deployment.storage = LocalStorage(path="/app")

    deploy(
        fx_flow_deployment,
        sp_stocks_flow_deployment,
        work_pool_name="my-docker-pool",
        image="securities-data-pipeline:latest",
        build=False,
        push=False,
    )
