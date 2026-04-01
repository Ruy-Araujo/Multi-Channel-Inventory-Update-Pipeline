from __future__ import annotations

import io
import json

import pandas as pd

from datamission_pipeline.validators import RawDatasetValidator


def test_validate_csv_payload_success() -> None:
    payload = (
        "order_id,timestamp,customer_id,product_category,price,quantity,store_location\n"
        "abc,2026-04-01T01:18:45.548709,9683,Home,10.5,2,Store A\n"
    ).encode("utf-8")

    validator = RawDatasetValidator()
    dataframe, report = validator.validate_and_parse(payload, "csv")

    assert not dataframe.empty
    assert report.is_valid


def test_validate_json_payload_success() -> None:
    payload = json.dumps(
        [
            {
                "order_id": "abc",
                "timestamp": "2026-04-01T01:18:45.548709",
                "customer_id": 9683,
                "product_category": "Home",
                "price": 10.5,
                "quantity": 2,
                "store_location": "Store A",
            }
        ]
    ).encode("utf-8")

    validator = RawDatasetValidator()
    dataframe, report = validator.validate_and_parse(payload, "json")

    assert len(dataframe) == 1
    assert report.is_valid


def test_validate_parquet_payload_success() -> None:
    dataframe = pd.DataFrame(
        {
            "order_id": ["abc"],
            "timestamp": ["2026-04-01T01:18:45.548709"],
            "customer_id": [9683],
            "product_category": ["Home"],
            "price": [10.5],
            "quantity": [2],
            "store_location": ["Store A"],
        }
    )
    buffer = io.BytesIO()
    dataframe.to_parquet(buffer, index=False)

    validator = RawDatasetValidator()
    parsed, report = validator.validate_and_parse(buffer.getvalue(), "parquet")

    assert len(parsed) == 1
    assert report.is_valid


def test_validate_missing_required_columns() -> None:
    payload = "order_id,timestamp\nabc,2026-04-01T01:18:45.548709\n".encode("utf-8")

    validator = RawDatasetValidator()
    _, report = validator.validate_and_parse(payload, "csv")

    assert not report.is_valid
    assert any(check.name == "required_columns" and not check.passed for check in report.checks)
