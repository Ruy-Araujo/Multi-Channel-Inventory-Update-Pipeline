from __future__ import annotations

from dataclasses import asdict, dataclass
import io
import json
from typing import Any

import pandas as pd


REQUIRED_COLUMNS = [
    "order_id",
    "timestamp",
    "customer_id",
    "product_category",
    "price",
    "quantity",
    "store_location",
]

SUPPORTED_FORMATS = {"csv", "json", "parquet"}


@dataclass
class ValidationCheck:
    name: str
    passed: bool
    details: str


@dataclass
class ValidationReport:
    is_valid: bool
    checks: list[ValidationCheck]

    def to_dict(self) -> dict[str, Any]:
        return {
            "is_valid": self.is_valid,
            "checks": [asdict(check) for check in self.checks],
        }


class RawDatasetValidator:
    def validate_and_parse(self, raw_payload: bytes, data_format: str) -> tuple[pd.DataFrame, ValidationReport]:
        checks: list[ValidationCheck] = []

        fmt = data_format.lower()
        if fmt not in SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported format: {data_format}")

        checks.append(
            ValidationCheck(
                name="payload_non_empty",
                passed=len(raw_payload) > 0,
                details=f"bytes={len(raw_payload)}",
            )
        )

        if len(raw_payload) == 0:
            return pd.DataFrame(), ValidationReport(is_valid=False, checks=checks)

        dataframe = self._parse_payload(raw_payload, fmt)
        dataframe.columns = [str(col).strip().lower() for col in dataframe.columns]

        checks.append(
            ValidationCheck(
                name="dataframe_non_empty",
                passed=not dataframe.empty,
                details=f"rows={len(dataframe)}",
            )
        )

        missing_columns = [col for col in REQUIRED_COLUMNS if col not in dataframe.columns]
        checks.append(
            ValidationCheck(
                name="required_columns",
                passed=len(missing_columns) == 0,
                details="missing=" + ",".join(missing_columns) if missing_columns else "ok",
            )
        )

        if not missing_columns:
            null_count = int(dataframe[REQUIRED_COLUMNS].isnull().sum().sum())
            checks.append(
                ValidationCheck(
                    name="critical_nulls",
                    passed=null_count == 0,
                    details=f"nulls={null_count}",
                )
            )

            type_ok, detail = self._validate_type_convertibility(dataframe)
            checks.append(
                ValidationCheck(
                    name="type_convertibility",
                    passed=type_ok,
                    details=detail,
                )
            )

            duplicate_count = int(dataframe.duplicated(subset=["order_id"]).sum())
            checks.append(
                ValidationCheck(
                    name="duplicate_order_id",
                    passed=duplicate_count == 0,
                    details=f"duplicates={duplicate_count}",
                )
            )

        is_valid = all(check.passed for check in checks)
        return dataframe, ValidationReport(is_valid=is_valid, checks=checks)

    def _parse_payload(self, raw_payload: bytes, data_format: str) -> pd.DataFrame:
        if data_format == "csv":
            return pd.read_csv(io.BytesIO(raw_payload), sep=None, engine="python")

        if data_format == "json":
            parsed = json.loads(raw_payload.decode("utf-8"))
            if isinstance(parsed, list):
                return pd.DataFrame(parsed)
            if isinstance(parsed, dict) and "records" in parsed and isinstance(parsed["records"], list):
                return pd.DataFrame(parsed["records"])
            if isinstance(parsed, dict):
                return pd.DataFrame([parsed])
            raise ValueError("Invalid JSON structure for tabular dataset")

        if data_format == "parquet":
            return pd.read_parquet(io.BytesIO(raw_payload))

        raise ValueError(f"Unsupported format: {data_format}")

    def _validate_type_convertibility(self, dataframe: pd.DataFrame) -> tuple[bool, str]:
        sample = dataframe.copy()

        ts = pd.to_datetime(sample["timestamp"], errors="coerce", utc=True)
        customer_id = pd.to_numeric(sample["customer_id"], errors="coerce")
        price = pd.to_numeric(sample["price"], errors="coerce")
        quantity = pd.to_numeric(sample["quantity"], errors="coerce")

        invalid = int(ts.isna().sum() + customer_id.isna().sum() + price.isna().sum() + quantity.isna().sum())
        return invalid == 0, f"invalid_conversions={invalid}"
