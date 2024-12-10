FROM ghcr.io/astral-sh/uv:python3.12-bookworm

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./

# Create venv, install dependencies, and create feature store
RUN uv venv --python 3.12 && \
    . .venv/bin/activate && \
    uv sync --dev --frozen

COPY src/ ./src/

RUN uv pip install -e . && \
    uv run python src/feature_repo/


# Entry point script to run modules
COPY docker-entrypoint.sh /
RUN chmod +x /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]
