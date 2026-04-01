from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from datamission_pipeline.config import Settings
from datamission_pipeline.pipeline import DatasetPipeline


@dataclass
class FakeResponse:
    payload: bytes
    status_code: int = 200
    content_type: str = "text/csv"
    elapsed_ms: int = 10
    downloaded_at: str = "2026-04-01T00:00:00+00:00"


class FakeClient:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def fetch_dataset(self, _project_id: str, _data_format: str) -> FakeResponse:
        return FakeResponse(payload=self._payload)


def test_pipeline_run_success(tmp_path: Path) -> None:
    settings = Settings(
        api_base_url="https://api.datamission.com.br",
        api_token="token",
        default_project_id="project",
        default_format="csv",
        base_dir=tmp_path,
        timeout_seconds=30,
        max_retries=1,
        retry_backoff_seconds=0.1,
    )
    settings.ensure_directories()

    payload = (
        "order_id,timestamp,customer_id,product_category,price,quantity,store_location\n"
        "abc,2026-04-01T01:18:45.548709,9683,Home,10.5,2,Store A\n"
    ).encode("utf-8")

    pipeline = DatasetPipeline(settings)
    pipeline.client = FakeClient(payload)

    raw_path, processed_path, metadata_path = pipeline.run(project_id="project", data_format="csv")

    assert raw_path is not None and raw_path.exists()
    assert processed_path is not None and processed_path.exists()
    assert metadata_path.exists()
