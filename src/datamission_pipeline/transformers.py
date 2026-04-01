from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from datamission_pipeline.validators import REQUIRED_COLUMNS


def normalize_dataframe(dataframe: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    normalized = dataframe.copy()
    normalized.columns = [str(col).strip().lower() for col in normalized.columns]

    normalized = normalized[REQUIRED_COLUMNS].copy()

    normalized["order_id"] = normalized["order_id"].astype(str).str.strip()
    normalized["product_category"] = normalized["product_category"].astype(str).str.strip()
    normalized["store_location"] = normalized["store_location"].astype(str).str.strip()

    normalized["timestamp"] = pd.to_datetime(normalized["timestamp"], errors="coerce", utc=True)
    normalized["customer_id"] = pd.to_numeric(normalized["customer_id"], errors="coerce")
    normalized["price"] = pd.to_numeric(normalized["price"], errors="coerce")
    normalized["quantity"] = pd.to_numeric(normalized["quantity"], errors="coerce")

    before_drop = len(normalized)
    normalized = normalized.dropna(subset=REQUIRED_COLUMNS)

    normalized["customer_id"] = normalized["customer_id"].astype(int)
    normalized["quantity"] = normalized["quantity"].astype(int)
    normalized["price"] = normalized["price"].astype(float)

    after_drop = len(normalized)
    stats = {
        "input_rows": before_drop,
        "output_rows": after_drop,
        "dropped_rows": before_drop - after_drop,
    }
    return normalized, stats


def enrich_inventory_dataframe(
    normalized_dataframe: pd.DataFrame,
    run_id: str,
    lead_time_days: int = 7,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int | float]]:
    """Create traceable intermediate dataset and key stock metrics.

    The function builds an aggregated demand table and joins it back to row-level data
    to compute derived columns such as days_of_coverage and safety_margin.
    """
    transformed_at = datetime.now(timezone.utc).isoformat()

    if normalized_dataframe.empty:
        empty_intermediate = normalized_dataframe.copy()
        derived_columns = [
            "order_date",
            "orders_count",
            "total_units",
            "avg_daily_demand",
            "avg_price",
            "inventory_on_hand",
            "days_of_coverage",
            "safety_margin",
            "needs_replenishment",
            "lineage_run_id",
            "lineage_transformed_at",
            "lineage_transformation_version",
        ]
        for column in derived_columns:
            if column not in empty_intermediate.columns:
                empty_intermediate[column] = pd.Series(dtype="object")

        # Keep lineage fields in the intermediate schema even when there are no rows.
        empty_intermediate = empty_intermediate.assign(
            lineage_run_id=run_id,
            lineage_transformed_at=transformed_at,
            lineage_transformation_version="v1",
        )

        empty_metrics = pd.DataFrame(
            columns=[
                "product_category",
                "store_location",
                "orders_count",
                "total_units",
                "avg_daily_demand",
                "avg_days_of_coverage",
                "avg_safety_margin",
            ]
        )
        return empty_intermediate, empty_metrics, {
            "intermediate_rows": 0,
            "metrics_rows": 0,
            "lead_time_days": lead_time_days,
            "lineage_run_id": run_id,
            "lineage_transformed_at": transformed_at,
        }

    enriched = normalized_dataframe.copy()
    enriched["order_date"] = enriched["timestamp"].dt.date

    grouped = (
        enriched.groupby(["product_category", "store_location"], dropna=False)
        .agg(
            orders_count=("order_id", "count"),
            total_units=("quantity", "sum"),
            active_days=("order_date", "nunique"),
            avg_price=("price", "mean"),
        )
        .reset_index()
    )
    grouped["active_days"] = grouped["active_days"].clip(lower=1)
    grouped["avg_daily_demand"] = grouped["total_units"] / grouped["active_days"]

    # Join aggregated demand back to transaction-level data for enrichment.
    enriched = enriched.merge(
        grouped[
            [
                "product_category",
                "store_location",
                "orders_count",
                "total_units",
                "avg_daily_demand",
                "avg_price",
            ]
        ],
        on=["product_category", "store_location"],
        how="left",
        validate="many_to_one",
    )

    enriched["inventory_on_hand"] = enriched["quantity"].astype(float)
    enriched["days_of_coverage"] = (
        enriched["inventory_on_hand"] / enriched["avg_daily_demand"].clip(lower=1e-9)
    )
    enriched["safety_margin"] = enriched["inventory_on_hand"] - (enriched["avg_daily_demand"] * lead_time_days)
    enriched["needs_replenishment"] = enriched["days_of_coverage"] < float(lead_time_days)

    enriched["lineage_run_id"] = run_id
    enriched["lineage_transformed_at"] = transformed_at
    enriched["lineage_transformation_version"] = "v1"

    metrics = (
        enriched.groupby(["product_category", "store_location"], dropna=False)
        .agg(
            orders_count=("order_id", "count"),
            total_units=("quantity", "sum"),
            avg_daily_demand=("avg_daily_demand", "first"),
            avg_days_of_coverage=("days_of_coverage", "mean"),
            avg_safety_margin=("safety_margin", "mean"),
        )
        .reset_index()
    )

    stats = {
        "intermediate_rows": int(len(enriched)),
        "metrics_rows": int(len(metrics)),
        "lead_time_days": lead_time_days,
        "lineage_run_id": run_id,
        "lineage_transformed_at": transformed_at,
    }
    return enriched, metrics, stats
