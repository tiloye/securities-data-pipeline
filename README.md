# Overview
A data pipeline is a series of steps that automate the extraction, transformation, and loading (ETL) of data from various sources into a data warehouse or data lake. This process is crucial for organizations that rely heavily on data-driven decision making. In investment management and algorithmic trading, a data pipeline is essential for:
* Gathering and analyzing historical market data to identify trends, patterns, and potential investment opportunities.
* Building and optimizing investment portfolios based on historical data and risk models.
* Developing and backtesting trading algorithms using historical market data.

In this project, I built a data pipeline to extract and store currency and stock price data in a data lake. The pipeline gathers and stores data for major forex currency pairs and stocks tracked by the SP500, SP400, and SP600 indices.

There are two folders in the datalake: symbols and price history. The symbols folder contains data about the ticker symbols for each stock while the price history folder contains the daily open, high, low, close, and volume data from January 1, 2000 up to the last time the pipeline ran.

# Tools Used
* yfinance: for extracting stocks and fx price data from Yahoo Finance.
* DuckDB: for extracting and loading data into an S3 bucket.
* MinIO: for storing extracted and transformed data in a local S3 bucket.
* Prefect: for workflow orchestration, monitoring, and scheduling.
* Docker: for containerzing the project for local and cloud deployment

# Project Structure
```
└── securities-data-pipeline
    └── pipeline
        └── main.py            # Main module for interacting with the pipeline
        └── price_history.py   # Contains code for extracting, tranforming, and loading price data
        └── s3_el.py           # Contains code for interacting with AWS/MinIO S3 bucket
        └── symbols.py         # Contains code for extracting, tranforming, loading symbols data
    └── .dockerignore
    └── .env
    └── .gitignore
    └── .python-version
    └── Dockerfile
    └── pyproject.toml
    └── README.md
    └── uv.lock
```

# Setting up and running the pipeline locally 
Ensure you have Docker installed on your system. The following steps were tested on Ubuntu 22.04.

1. [Install](https://min.io/docs/minio/container/index.html) MinIO via Docker or directly, depending on your operating system.
2. Run Prefect server in a Docker container:
    ```
    docker run \
     -v YOUR_VOLUME_DIR:root/.prefect \
     -e PREFECT_SERVER_API_HOST=0.0.0.0 \
     -e PREFECT_UI_URL=http://localhost:4200/api \
     -e PREFECT_API_URL=http://localhost:4200/api \
     -p 4200:4200 \
     --restart=always \
     prefecthq/prefect:3.1.7-python3.12 \
     prefect server start
    ```

3. Clone the repository:
    ```
    git clone https://github.com/tiloye/securities-data-pipeline.git
    ```
4. Create ".env" file with the following values in the project directory:
    ```
    AWS_ACCESS_KEY=your_aws/minio_access_key
    AWS_SECRET_KEY=your_aws/minio_secret_key
    S3_ENDPOINT=your_aws/minio_s3_endpoint_url
    
    PREFECT_API_URL=your_prefect_api_url
    ```
5. Create docker image for the pipeline:
    ```
    docker build -t securities-data-pipeline .
    ```
6. Start a long-running container with the image:
    ```
    docker run --restart=always --network=host securities-data-pipeline
    ```