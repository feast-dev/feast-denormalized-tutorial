#!/bin/bash
set -e

. .venv/bin/activate

case "$1" in
  "generator")
    shift
    python -m src.session_generator "$@"
    ;;
  "pipeline")
    shift
    if [ -z "$(ls -A src/feature_repo/data/)" ]; then
      echo "Data directory empty, initializing feature store..."
      uv run python src/feature_repo
    fi
    python -m src.pipeline "$@"
    ;;
  *)
    echo "Usage: $0 {generator|pipeline} [args]"
    exit 1
    ;;
esac
