#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f .env ]]; then
  # shellcheck disable=SC2046
  export $(grep -v '^#' .env | xargs)
fi

python -m datamission_pipeline.cli run \
  --project-id "${DATAMISSION_PROJECT_ID}" \
  --format "${DATAMISSION_DEFAULT_FORMAT:-parquet}"
