from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any


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
    processed_file_path: str | None = None
    raw_row_count: int | None = None
    row_count: int | None = None
    dropped_rows: int | None = None
    validation_results: dict[str, Any] = field(default_factory=dict)
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
