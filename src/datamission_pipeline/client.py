from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import time

import requests


@dataclass
class DatasetResponse:
    payload: bytes
    status_code: int
    content_type: str
    elapsed_ms: int
    downloaded_at: str


class DatasetApiClient:
    def __init__(
        self,
        base_url: str,
        token: str,
        timeout_seconds: int = 30,
        max_retries: int = 3,
        retry_backoff_seconds: float = 1.5,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds

        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Accept": "*/*",
        })

    def fetch_dataset(self, project_id: str, data_format: str) -> DatasetResponse:
        url = f"{self.base_url}/projects/{project_id}/dataset"
        params = {"format": data_format}

        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout_seconds)

                if response.status_code >= 500 and attempt < self.max_retries:
                    time.sleep(self.retry_backoff_seconds * attempt)
                    continue

                response.raise_for_status()

                elapsed_ms = int(response.elapsed.total_seconds() * 1000)
                content_type = response.headers.get("Content-Type", "application/octet-stream")

                return DatasetResponse(
                    payload=response.content,
                    status_code=response.status_code,
                    content_type=content_type,
                    elapsed_ms=elapsed_ms,
                    downloaded_at=datetime.now(timezone.utc).isoformat(),
                )
            except requests.RequestException:
                if attempt == self.max_retries:
                    raise
                time.sleep(self.retry_backoff_seconds * attempt)

        raise RuntimeError("Failed to download dataset after configured retry attempts")
