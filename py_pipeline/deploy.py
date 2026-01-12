from datetime import timedelta

from prefect import deploy
from prefect.events import DeploymentCompoundTrigger, EventTrigger
from prefect.runner.storage import LocalStorage

from py_pipeline.main import dbt_runner, main_fx, main_sp_stocks

if __name__ == "__main__":
    schedule = "0 0 * * 2-6"
    fx_flow_deployment = main_fx.to_deployment("fx-data-pipeline", cron=schedule)
    fx_flow_deployment.storage = LocalStorage(path="/app")

    sp_stocks_flow_deployment = main_sp_stocks.to_deployment(
        "sp-stocks-data-pipeline", cron=schedule
    )
    sp_stocks_flow_deployment.storage = LocalStorage(path="/app")

    dbt_runner_deployment = dbt_runner.to_deployment(
        "dbt-dw-transformer",
        triggers=[
            DeploymentCompoundTrigger(
                name="sec-dw-transformation-runner",
                require="all",
                within=timedelta(minutes=10),
                triggers=[
                    EventTrigger(
                        expect={"prefect.flow-run.Completed"},
                        match_related={"prefect.resource.name": "fx-data-pipeline"},
                    ),
                    EventTrigger(
                        expect={"prefect.flow-run.Completed"},
                        match_related={
                            "prefect.resource.name": "sp-stocks-data-pipeline"
                        },
                    ),
                ],
            )
        ],
    )
    dbt_runner_deployment.storage = LocalStorage(path="/app")

    deploy(
        fx_flow_deployment,
        sp_stocks_flow_deployment,
        dbt_runner_deployment,
        work_pool_name="docker-pool",
        image="securities-data-pipeline:latest",
        build=False,
        push=False,
    )
