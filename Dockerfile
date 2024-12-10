FROM ghcr.io/astral-sh/uv:python3.12-bookworm

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

# Combine RUN commands and cleanup in same layer
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

COPY pyproject.toml uv.lock ./

# Combine Python setup steps
RUN --mount=type=cache,target=/root/.cache/uv \
    uv venv --python 3.12 && \
    . .venv/bin/activate && \
    uv sync --frozen --compile-bytecode

COPY src/ ./src/
COPY docker-entrypoint.sh /

# Combine install and permissions in one layer
RUN uv pip install -e . && \
    chmod +x /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]
