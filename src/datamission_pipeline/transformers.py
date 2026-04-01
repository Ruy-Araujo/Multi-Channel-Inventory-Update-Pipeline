from __future__ import annotations

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
