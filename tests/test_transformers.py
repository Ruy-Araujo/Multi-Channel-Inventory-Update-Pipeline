from __future__ import annotations

import pandas as pd

from datamission_pipeline.transformers import normalize_dataframe


def test_normalize_dataframe_returns_expected_schema() -> None:
    df = pd.DataFrame(
        {
            "order_id": ["abc"],
            "timestamp": ["2026-04-01T01:18:45.548709"],
            "customer_id": ["9683"],
            "product_category": ["Home"],
            "price": ["10.5"],
            "quantity": ["2"],
            "store_location": ["Store A"],
        }
    )

    normalized, stats = normalize_dataframe(df)

    assert list(normalized.columns) == [
        "order_id",
        "timestamp",
        "customer_id",
        "product_category",
        "price",
        "quantity",
        "store_location",
    ]
    assert stats["input_rows"] == 1
    assert stats["output_rows"] == 1
    assert stats["dropped_rows"] == 0


def test_normalize_dataframe_drops_invalid_rows() -> None:
    df = pd.DataFrame(
        {
            "order_id": ["abc", "def"],
            "timestamp": ["2026-04-01T01:18:45.548709", "invalid"],
            "customer_id": ["9683", "123"],
            "product_category": ["Home", "Fashion"],
            "price": ["10.5", "2.5"],
            "quantity": ["2", "x"],
            "store_location": ["Store A", "Store B"],
        }
    )

    normalized, stats = normalize_dataframe(df)

    assert len(normalized) == 1
    assert stats["dropped_rows"] == 1
