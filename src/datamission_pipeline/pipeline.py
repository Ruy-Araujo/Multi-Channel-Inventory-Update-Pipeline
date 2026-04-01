from __future__ import annotations

import hashlib
import json
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from datamission_pipeline.client import DatasetApiClient
from datamission_pipeline.config import Settings
from datamission_pipeline.metadata import PipelineAlert, RunMetadata, write_metadata
from datamission_pipeline.transformers import enrich_inventory_dataframe, normalize_dataframe
from datamission_pipeline.validators import RawDatasetValidator, ValidationReport


logger = logging.getLogger(__name__)


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
        intermediate_file_path: Path | None = None
        processed_file_path: Path | None = None
        metrics_file_path: Path | None = None
        published_file_path: Path | None = None
        published_metrics_file_path: Path | None = None
        publish_manifest_path: Path | None = None
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
            metadata.alerts.extend(
                self._build_validation_alerts(
                    validation_report=validation_report,
                    raw_row_count=len(raw_df),
                )
            )

            if not validation_report.is_valid:
                raise ValueError("Payload integrity validation failed")

            raw_file_path = self._save_raw_payload(response.payload, project_id, data_format)
            metadata.raw_file_path = str(raw_file_path)

            normalized_df, stats = normalize_dataframe(raw_df)
            metadata.row_count = stats["output_rows"]
            metadata.dropped_rows = stats["dropped_rows"]
            metadata.alerts.extend(self._build_completeness_alerts(stats=stats, raw_row_count=len(raw_df)))

            enriched_df, metrics_df, transform_stats = enrich_inventory_dataframe(normalized_df, run_id=run_id)
            metadata.transformation_stats = transform_stats
            metadata.derived_columns = [
                "orders_count",
                "total_units",
                "avg_daily_demand",
                "inventory_on_hand",
                "days_of_coverage",
                "safety_margin",
                "needs_replenishment",
                "lineage_run_id",
                "lineage_transformed_at",
                "lineage_transformation_version",
            ]

            intermediate_file_path = self._save_intermediate_dataframe(enriched_df, run_id)
            metadata.intermediate_file_path = str(intermediate_file_path)

            processed_file_path = self._save_processed_dataframe(enriched_df, run_id)
            metadata.processed_file_path = str(processed_file_path)

            metrics_file_path = self._save_metrics_dataframe(metrics_df, run_id)
            metadata.metrics_file_path = str(metrics_file_path)

            published_file_path, published_metrics_file_path, publish_manifest_path = self._publish_results(
                run_id=run_id,
                processed_file_path=processed_file_path,
                metrics_file_path=metrics_file_path,
                project_id=project_id,
                data_format=data_format,
            )
            metadata.published_file_path = str(published_file_path)
            metadata.published_metrics_file_path = str(published_metrics_file_path)
            metadata.publish_manifest_path = str(publish_manifest_path)

            self._log_alerts(run_id=run_id, alerts=metadata.alerts)

            metadata.finish("success")
            logger.info("Pipeline execution completed successfully", extra={"run_id": run_id, "rows": metadata.row_count})

        except Exception as exc:
            self._log_alerts(run_id=run_id, alerts=metadata.alerts)
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

    def _save_intermediate_dataframe(self, dataframe, run_id: str) -> Path:
        output_path = self.settings.processed_dir / f"intermediate_{run_id}.parquet"
        dataframe.to_parquet(output_path, index=False)
        return output_path

    def _save_processed_dataframe(self, dataframe, run_id: str) -> Path:
        output_path = self.settings.processed_dir / f"{run_id}.parquet"
        dataframe.to_parquet(output_path, index=False)
        return output_path

    def _save_metrics_dataframe(self, dataframe, run_id: str) -> Path:
        output_path = self.settings.processed_dir / f"metrics_{run_id}.parquet"
        dataframe.to_parquet(output_path, index=False)
        return output_path

    def _publish_results(
        self,
        run_id: str,
        processed_file_path: Path,
        metrics_file_path: Path,
        project_id: str,
        data_format: str,
    ) -> tuple[Path, Path, Path]:
        published_file_path = self.settings.published_dir / "latest_enriched.parquet"
        published_metrics_file_path = self.settings.published_dir / "latest_metrics.parquet"
        publish_manifest_path = self.settings.published_dir / f"manifest_{run_id}.json"

        shutil.copy2(processed_file_path, published_file_path)
        shutil.copy2(metrics_file_path, published_metrics_file_path)

        manifest = {
            "run_id": run_id,
            "project_id": project_id,
            "data_format": data_format,
            "published_at": datetime.now(timezone.utc).isoformat(),
            "published_file_path": str(published_file_path),
            "published_metrics_file_path": str(published_metrics_file_path),
            "source_processed_file": str(processed_file_path),
            "source_metrics_file": str(metrics_file_path),
        }
        publish_manifest_path.write_text(json.dumps(manifest, ensure_ascii=True, indent=2), encoding="utf-8")
        return published_file_path, published_metrics_file_path, publish_manifest_path

    def _build_validation_alerts(self, validation_report: ValidationReport, raw_row_count: int) -> list[PipelineAlert]:
        alerts: list[PipelineAlert] = []
        for check in validation_report.checks:
            if check.passed:
                continue
            severity = "error" if check.name in {"required_columns", "type_convertibility"} else "warning"
            alerts.append(
                PipelineAlert(
                    code=f"validation_{check.name}",
                    severity=severity,
                    message=f"Validation check failed: {check.name}",
                    details={"check_details": check.details, "raw_row_count": raw_row_count},
                )
            )
        return alerts

    def _build_completeness_alerts(self, stats: dict[str, int], raw_row_count: int) -> list[PipelineAlert]:
        alerts: list[PipelineAlert] = []

        if raw_row_count < self.settings.min_expected_rows:
            alerts.append(
                PipelineAlert(
                    code="dataset_incomplete_low_rows",
                    severity="warning",
                    message="Downloaded dataset has fewer rows than expected minimum",
                    details={
                        "raw_row_count": raw_row_count,
                        "min_expected_rows": self.settings.min_expected_rows,
                    },
                )
            )

        dropped_rows = stats.get("dropped_rows", 0)
        input_rows = max(stats.get("input_rows", 0), 1)
        dropped_ratio = dropped_rows / input_rows
        if dropped_ratio > self.settings.max_dropped_rows_ratio:
            alerts.append(
                PipelineAlert(
                    code="dataset_incomplete_high_drop_ratio",
                    severity="warning",
                    message="Normalization dropped more rows than allowed threshold",
                    details={
                        "dropped_rows": dropped_rows,
                        "input_rows": input_rows,
                        "dropped_rows_ratio": dropped_ratio,
                        "max_dropped_rows_ratio": self.settings.max_dropped_rows_ratio,
                    },
                )
            )

        return alerts

    def _log_alerts(self, run_id: str, alerts: list[PipelineAlert]) -> None:
        for alert in alerts:
            logger.warning(
                "Pipeline alert",
                extra={
                    "run_id": run_id,
                    "code": alert.code,
                    "severity": alert.severity,
                    "message": alert.message,
                    "details": alert.details,
                },
            )
