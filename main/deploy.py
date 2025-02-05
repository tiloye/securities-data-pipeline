from prefect import serve

from .main import etl

if __name__ == "__main__":
    fx_flow = etl.to_deployment(
        "fx-data-pipeline", cron="0 0 * * 1-5", parameters={"asset_category": "fx"}
    )
    sp_stocks_flow = etl.to_deployment(
        "sp-stocks-data-pipeline",
        cron="0 0 * * 1-5",
        parameters={"asset_category": "sp_stocks"},
    )
    serve(fx_flow, sp_stocks_flow)
