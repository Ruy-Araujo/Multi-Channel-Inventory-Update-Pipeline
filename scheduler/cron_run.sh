#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f .env ]]; then
  # shellcheck disable=SC2046
  export $(grep -v '^#' .env | xargs)
fi

args=(run --format "${DATAMISSION_DEFAULT_FORMAT:-parquet}")
if [[ -n "${DATAMISSION_PROJECT_ID:-}" ]]; then
  args+=(--project-id "${DATAMISSION_PROJECT_ID}")
fi

PYTHONPATH=src python -m datamission_pipeline.cli "${args[@]}"
