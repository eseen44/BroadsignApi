"""
Silver: fill_rate_clean

fill_rate wzbogacone o daty line_start/line_end z proposal_items.
Jeden wiersz = jeden line item w danym dniu.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
from Pipeline.silver.utils import read_bronze, save_silver


def build_fill_rate() -> pd.DataFrame:
    fr = read_bronze("fill_rate")

    # Rozpakuj JSON buy_details
    from Pipeline.silver.utils import unpack_json_col
    fr = unpack_json_col(fr, "buy_details", prefix="buy")
    fr = unpack_json_col(fr, "priority", prefix="priority")

    # Wzbogać o dodatkowe pola z proposal_items (status_name, owner_user_id)
    # fill_rate już ma owner_name, więc nie duplikujemy
    items = read_bronze("proposal_items")[
        ["id", "status_name", "owner_user_id"]
    ].rename(columns={
        "id":          "line_id",
        "status_name": "line_status",
    })

    df = fr.merge(items, on="line_id", how="left")

    # Typy dat
    for col in ["start_date", "end_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    df = df[[c for c in df.columns if c != "_fetched_at"]]

    save_silver(df, "fill_rate_clean")
    return df


if __name__ == "__main__":
    df = build_fill_rate()
    print(df.head(5).to_string())
    print(f"\nRows: {len(df)}, date range: {df['start_date'].min().date()} -> {df['end_date'].max().date()}")
