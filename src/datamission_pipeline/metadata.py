from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any


@dataclass
class PipelineAlert:
    code: str
    severity: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class RunMetadata:
    run_id: str
    project_id: str
    data_format: str
    started_at: str
    downloaded_at: str | None = None
    status_http: int | None = None
    elapsed_ms: int | None = None
    checksum_sha256: str | None = None
    raw_file_path: str | None = None
    intermediate_file_path: str | None = None
    processed_file_path: str | None = None
    metrics_file_path: str | None = None
    published_file_path: str | None = None
    published_metrics_file_path: str | None = None
    publish_manifest_path: str | None = None
    raw_row_count: int | None = None
    row_count: int | None = None
    dropped_rows: int | None = None
    transformation_stats: dict[str, Any] = field(default_factory=dict)
    derived_columns: list[str] = field(default_factory=list)
    validation_results: dict[str, Any] = field(default_factory=dict)
    alerts: list[PipelineAlert] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    status: str = "running"
    finished_at: str | None = None

    def finish(self, status: str) -> None:
        self.status = status
        self.finished_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def write_metadata(metadata: RunMetadata, logs_dir: Path) -> Path:
    logs_dir.mkdir(parents=True, exist_ok=True)
    output_path = logs_dir / f"run_{metadata.run_id}.json"
    output_path.write_text(json.dumps(metadata.to_dict(), ensure_ascii=True, indent=2), encoding="utf-8")
    return output_path
