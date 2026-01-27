# Use the official Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# Set the working directory
WORKDIR /app

# Set uv environment variables
ENV UV_SYSTEM_PYTHON=1
ENV UV_PROJECT_ENVIRONMENT="/usr/local/"

# Install the project's dependencies using the lockfile and settings
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=/uv.lock,target=/uv.lock \
    --mount=type=bind,source=/pyproject.toml,target=/pyproject.toml \
    uv sync --frozen --no-install-project --no-dev

# Then, add the rest of the project source code and install it
# Installing separately from its dependencies allows optimal layer caching
COPY . /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev


# Set project environment variables
ARG ENV_NAME
ARG S3_ENDPOINT
ARG BUCKET_NAME
ARG PREFECT_API_URL

ENV ENV_NAME=${ENV_NAME}
ENV S3_ENDPOINT=$S3_ENDPOINT
ENV BUCKET_NAME=$BUCKET_NAME
ENV PREFECT_API_URL=$PREFECT_API_URL