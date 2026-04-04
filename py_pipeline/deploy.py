from __future__ import annotations

from typing import TYPE_CHECKING
from datetime import timedelta
from pathlib import Path

from prefect import deploy
from prefect.events import DeploymentCompoundTrigger, EventTrigger
from prefect.runner.storage import LocalStorage, GitRepository
from py_pipeline.orchestration import dbt_runner, etl_flow

if TYPE_CHECKING:
    from prefect import Flow


def get_pip_requirements(file_name: str = "requirements.txt") -> list[str]:
    """Parse requirements.txt into a list of package specifiers, skipping comments and local edits."""
    file_path = Path(__file__).parent.parent / file_name

    if not file_path.exists():
        return ["prefect>=3.0.0", "marvin"]

    requirements = []
    with open(file_path, "r") as f:
        for line in f:
            item = line.strip()
            # Default uv export format: skip empty lines, comments, and local paths
            if (
                not item
                or item.startswith("#")
                or item.startswith("-e")
                or item.startswith("--")
            ):
                continue
            item = item.split()[0]
            requirements.append(item)
    return requirements


def get_dynamic_deployment(
    flow: Flow,
    deployment_name: str,
    cron: str,
    parameters: dict | None = None,
    work_pool_type: str = "hybrid",
):
    if work_pool_type == "hybrid":
        deployment = flow.to_deployment(
            name=deployment_name,
            cron=cron,
            parameters=parameters,
        )
        deployment.storage = LocalStorage(path="/app")
    elif work_pool_type == "managed":
        from py_pipeline.config import S3_ENDPOINT, BUCKET_NAME, DB_TYPE, DB_NAME

        deployment = flow.from_source(
            source=GitRepository(
                url="https://github.com/tiloye/securities-data-pipeline.git",
                branch="dev",
            ),
            entrypoint=f"py_pipeline/orchestration.py:{flow.fn.__name__}",
        ).to_deployment(
            name=deployment_name,
            cron=cron,
            parameters=parameters,
            job_variables={
                "pip_packages": get_pip_requirements(),
                "env": {
                    "ENV_NAME": "cloud",
                    "S3_ENDPOINT": S3_ENDPOINT,
                    "BUCKET_NAME": BUCKET_NAME,
                    "DB_TYPE": DB_TYPE,
                    "DB_NAME": DB_NAME,
                },
            },
        )

    if "dbt" in flow.name:
        deployment.triggers = [
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
        ]

    return deployment


def run_dynamic_deployment(
    cron: str, work_pool_name: str, work_pool_type: str = "hybrid"
):
    if work_pool_type not in ["managed", "hybrid"]:
        raise ValueError(f"Unsupported deployment work pool type: {work_pool_type}")

    fx_flow_deployment = get_dynamic_deployment(
        etl_flow,
        "fx-data-pipeline",
        cron,
        parameters={"asset_category": "fx"},
        work_pool_type=work_pool_type,
    )

    sp_stocks_flow_deployment = get_dynamic_deployment(
        etl_flow,
        "sp-stocks-data-pipeline",
        cron,
        parameters={"asset_category": "sp_stocks"},
        work_pool_type=work_pool_type,
    )

    dbt_runner_deployment = get_dynamic_deployment(
        dbt_runner,
        "dbt-dw-transformer",
        cron,
        work_pool_type=work_pool_type,
    )

    if work_pool_type == "hybrid":
        deploy(
            fx_flow_deployment,
            sp_stocks_flow_deployment,
            dbt_runner_deployment,
            work_pool_name=work_pool_name,
            image="securities-data-pipeline:latest",
            build=False,
            push=False,
        )
    elif work_pool_type == "managed":
        deploy(
            fx_flow_deployment,
            sp_stocks_flow_deployment,
            dbt_runner_deployment,
            work_pool_name=work_pool_name,
        )


if __name__ == "__main__":
    cron = "0 0 * * 2-6"
    work_pool_name = "docker-pool"
    work_pool_type = "managed"

    run_dynamic_deployment(cron, work_pool_name, work_pool_type)
