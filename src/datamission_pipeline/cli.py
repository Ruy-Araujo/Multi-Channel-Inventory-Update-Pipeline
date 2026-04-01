from __future__ import annotations

import argparse
import json
import logging
import sys

from datamission_pipeline.config import load_settings
from datamission_pipeline.pipeline import DatasetPipeline


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="DataMission Pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run the full pipeline")
    run_parser.add_argument("--project-id", required=False, help="DataMission project ID")
    run_parser.add_argument(
        "--format",
        required=False,
        default=None,
        choices=["csv", "json", "parquet"],
        help="Dataset format returned by the API",
    )

    return parser


def main() -> int:
    configure_logging()
    parser = build_parser()
    args = parser.parse_args()

    try:
        settings = load_settings()
        project_id = args.project_id or settings.default_project_id
        if not project_id:
            raise ValueError("Provide --project-id or set DATAMISSION_PROJECT_ID in the environment")

        data_format = args.format or settings.default_format

        pipeline = DatasetPipeline(settings)
        raw_path, processed_path, metadata_path = pipeline.run(project_id=project_id, data_format=data_format)

        result = {
            "raw_path": str(raw_path) if raw_path else None,
            "processed_path": str(processed_path) if processed_path else None,
            "metadata_path": str(metadata_path),
        }
        print(json.dumps(result, ensure_ascii=True, indent=2))
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
