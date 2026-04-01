from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from pydantic import BaseModel, Field


class Settings(BaseModel):
    api_base_url: str = Field(default="https://api.datamission.com.br")
    api_token: str
    default_project_id: str | None = None
    default_format: str = Field(default="parquet")
    base_dir: Path = Field(default=Path("."))
    timeout_seconds: int = Field(default=30)
    max_retries: int = Field(default=3)
    retry_backoff_seconds: float = Field(default=1.5)
    min_expected_rows: int = Field(default=1)
    max_dropped_rows_ratio: float = Field(default=0.25)

    @property
    def raw_dir(self) -> Path:
        return self.base_dir / "data" / "raw"

    @property
    def processed_dir(self) -> Path:
        return self.base_dir / "data" / "processed"

    @property
    def logs_dir(self) -> Path:
        return self.base_dir / "data" / "logs"

    @property
    def published_dir(self) -> Path:
        return self.base_dir / "data" / "published"

    def ensure_directories(self) -> None:
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.published_dir.mkdir(parents=True, exist_ok=True)


def load_settings() -> Settings:
    load_dotenv()

    token = os.getenv("DATAMISSION_API_TOKEN")
    if not token:
        raise ValueError("DATAMISSION_API_TOKEN is not set in the environment")

    base_dir = Path(os.getenv("PIPELINE_BASE_DIR", ".")).resolve()

    settings = Settings(
        api_base_url=os.getenv("DATAMISSION_API_BASE_URL", "https://api.datamission.com.br"),
        api_token=token,
        default_project_id=os.getenv("DATAMISSION_PROJECT_ID"),
        default_format=os.getenv("DATAMISSION_DEFAULT_FORMAT", "parquet").lower(),
        base_dir=base_dir,
        timeout_seconds=int(os.getenv("PIPELINE_TIMEOUT_SECONDS", "30")),
        max_retries=int(os.getenv("PIPELINE_MAX_RETRIES", "3")),
        retry_backoff_seconds=float(os.getenv("PIPELINE_RETRY_BACKOFF_SECONDS", "1.5")),
        min_expected_rows=int(os.getenv("PIPELINE_MIN_EXPECTED_ROWS", "1")),
        max_dropped_rows_ratio=float(os.getenv("PIPELINE_MAX_DROPPED_ROWS_RATIO", "0.25")),
    )
    settings.ensure_directories()
    return settings
