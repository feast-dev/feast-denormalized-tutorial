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
    python -m src.pipeline "$@"
    ;;
  *)
    echo "Usage: $0 {generator|pipeline} [args]"
    exit 1
    ;;
esac