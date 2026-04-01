from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from datamission_pipeline.client import DatasetApiClient
from datamission_pipeline.config import Settings
from datamission_pipeline.metadata import RunMetadata, write_metadata
from datamission_pipeline.transformers import normalize_dataframe
from datamission_pipeline.validators import RawDatasetValidator


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")


class DatasetPipeline:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = DatasetApiClient(
            base_url=settings.api_base_url,
            token=settings.api_token,
            timeout_seconds=settings.timeout_seconds,
            max_retries=settings.max_retries,
            retry_backoff_seconds=settings.retry_backoff_seconds,
        )
        self.validator = RawDatasetValidator()

    def run(self, project_id: str, data_format: str) -> tuple[Path | None, Path | None, Path]:
        run_id = str(uuid4())
        metadata = RunMetadata(
            run_id=run_id,
            project_id=project_id,
            data_format=data_format,
            started_at=datetime.now(timezone.utc).isoformat(),
        )

        raw_file_path: Path | None = None
        processed_file_path: Path | None = None
        metadata_path: Path

        try:
            logger.info("Starting dataset download", extra={"project_id": project_id, "format": data_format})
            response = self.client.fetch_dataset(project_id=project_id, data_format=data_format)

            metadata.downloaded_at = response.downloaded_at
            metadata.status_http = response.status_code
            metadata.elapsed_ms = response.elapsed_ms
            metadata.checksum_sha256 = hashlib.sha256(response.payload).hexdigest()

            # Validate in memory before persisting raw payload locally.
            raw_df, validation_report = self.validator.validate_and_parse(response.payload, data_format)
            metadata.validation_results = validation_report.to_dict()
            metadata.raw_row_count = len(raw_df)

            if not validation_report.is_valid:
                raise ValueError("Payload integrity validation failed")

            raw_file_path = self._save_raw_payload(response.payload, project_id, data_format)
            metadata.raw_file_path = str(raw_file_path)

            normalized_df, stats = normalize_dataframe(raw_df)
            metadata.row_count = stats["output_rows"]
            metadata.dropped_rows = stats["dropped_rows"]

            processed_file_path = self._save_processed_dataframe(normalized_df, run_id)
            metadata.processed_file_path = str(processed_file_path)

            metadata.finish("success")
            logger.info("Pipeline execution completed successfully", extra={"run_id": run_id, "rows": metadata.row_count})

        except Exception as exc:
            metadata.errors.append(str(exc))
            metadata.finish("failed")
            logger.exception("Pipeline execution failed")
            raise
        finally:
            metadata_path = write_metadata(metadata, self.settings.logs_dir)

        return raw_file_path, processed_file_path, metadata_path

    def _save_raw_payload(self, payload: bytes, project_id: str, data_format: str) -> Path:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        extension = "json" if data_format == "json" else data_format
        output_path = self.settings.raw_dir / f"{project_id}_{ts}.{extension}"
        output_path.write_bytes(payload)
        return output_path

    def _save_processed_dataframe(self, dataframe, run_id: str) -> Path:
        output_path = self.settings.processed_dir / f"{run_id}.parquet"
        dataframe.to_parquet(output_path, index=False)
        return output_path
