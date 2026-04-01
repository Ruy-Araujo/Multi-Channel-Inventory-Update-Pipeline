from __future__ import annotations

import pandas as pd

from datamission_pipeline.transformers import enrich_inventory_dataframe, normalize_dataframe


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


def test_enrich_inventory_dataframe_adds_derived_columns() -> None:
    df = pd.DataFrame(
        {
            "order_id": ["a", "b"],
            "timestamp": [
                "2026-04-01T01:18:45.548709",
                "2026-04-02T01:18:45.548709",
            ],
            "customer_id": ["9683", "1111"],
            "product_category": ["Home", "Home"],
            "price": ["10.5", "8.5"],
            "quantity": ["2", "4"],
            "store_location": ["Store A", "Store A"],
        }
    )
    normalized, _ = normalize_dataframe(df)

    intermediate, metrics, stats = enrich_inventory_dataframe(normalized, run_id="run-123")

    assert len(intermediate) == 2
    assert len(metrics) == 1
    assert "days_of_coverage" in intermediate.columns
    assert "safety_margin" in intermediate.columns
    assert "lineage_run_id" in intermediate.columns
    assert intermediate["lineage_run_id"].iloc[0] == "run-123"
    assert stats["intermediate_rows"] == 2
    assert stats["metrics_rows"] == 1
