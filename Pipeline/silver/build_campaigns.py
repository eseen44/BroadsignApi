"""
Silver: campaigns = proposal_items × proposals

Centralny stół analityczny dla Power BI.
Jeden wiersz = jeden line item (proposal_item), wzbogacony o pola z kampanii (proposal).

Klucz joinu: proposal_items.campaign_id → proposals.id
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
from Pipeline.silver.utils import read_bronze, save_silver, unpack_json_col


def build_campaigns() -> pd.DataFrame:
    items = read_bronze("proposal_items")
    props = read_bronze("proposals")
    users = read_bronze("users")

    # ----------------------------------------------------------------
    # Rozpakuj JSON-string kolumny w proposal_items
    # ----------------------------------------------------------------
    for col, pfx in [("buy_details", "buy"), ("performance", "perf"),
                     ("delivery", "del"), ("priority", "priority")]:
        items = unpack_json_col(items, col, prefix=pfx)

    # Rozpakuj performance z proposals
    props = unpack_json_col(props, "performance", prefix="camp_perf")

    # ----------------------------------------------------------------
    # Wybierz kolumny z proposals (prefiks camp_)
    # ----------------------------------------------------------------
    prop_cols = {
        "id":              "campaign_id",
        "name":            "campaign_name",
        "status":          "campaign_status",
        "client_id":       "client_id",
        "client_name":     "client_name",
        "advertiser":      "advertiser",
        "contract_id":     "contract_id",
        "contract_number": "contract_number",
        "start_date":      "campaign_start",
        "end_date":        "campaign_end",
        "price":           "campaign_price",
        "discount":        "campaign_discount",
        "suggested_price": "campaign_suggested_price",
        "owner_user_id":   "campaign_owner_id",
        "owner_user_name": "campaign_owner_name",
        "creation_user_name": "campaign_creator",
    }
    # Dołącz tylko kolumny które istnieją
    existing_prop_cols = {k: v for k, v in prop_cols.items() if k in props.columns}
    props_slim = props[list(existing_prop_cols.keys())].rename(columns=existing_prop_cols)

    # ----------------------------------------------------------------
    # Wybierz kolumny z proposal_items
    # ----------------------------------------------------------------
    item_cols = [
        "id", "campaign_id", "name", "type_of_buy", "priority", "is_preemptible",
        "status_id", "status_name", "start_date", "end_date",
        "price", "suggested_price", "slot_duration",
        "screen_count", "group_count",
        "owner_user_id", "owner_name", "creator",
        "client_id", "client_name", "advertiser", "buyer",
    ]
    # Dołącz też rozpakowane kolumny buy_ perf_ del_
    extra_cols = [c for c in items.columns if c.startswith(("buy_", "perf_", "del_"))]
    item_cols_existing = [c for c in item_cols if c in items.columns] + extra_cols

    items_slim = items[item_cols_existing].rename(columns={
        "id":           "line_item_id",
        "name":         "line_item_name",
        "start_date":   "line_start",
        "end_date":     "line_end",
        "price":        "line_price",
        "suggested_price": "line_suggested_price",
        "owner_user_id": "owner_user_id",
        "owner_name":   "owner_name",
    })

    # ----------------------------------------------------------------
    # JOIN items → proposals
    # ----------------------------------------------------------------
    df = items_slim.merge(props_slim, on="campaign_id", how="left", suffixes=("_item", "_camp"))

    # Kolumny zduplikowane (advertiser, client_id, client_name) —
    # używamy wartości z proposals (kampania), fallback na wartość z line item
    for base in ["advertiser", "client_id", "client_name"]:
        col_camp = f"{base}_camp"
        col_item = f"{base}_item"
        if col_camp in df.columns and col_item in df.columns:
            df[base] = df[col_camp].fillna(df[col_item])
            df = df.drop(columns=[col_camp, col_item])
        elif col_camp in df.columns:
            df = df.rename(columns={col_camp: base})
        elif col_item in df.columns:
            df = df.rename(columns={col_item: base})

    # ----------------------------------------------------------------
    # Wzbogać o email właściciela (z users)
    # ----------------------------------------------------------------
    if "email" in users.columns:
        user_lookup = users[["id", "email"]].rename(columns={"id": "owner_user_id", "email": "owner_email"})
        df = df.merge(user_lookup, on="owner_user_id", how="left")

    # ----------------------------------------------------------------
    # Typy dat
    # ----------------------------------------------------------------
    for date_col in ["line_start", "line_end", "campaign_start", "campaign_end"]:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    # ----------------------------------------------------------------
    # Usuń kolumny _fetched_at z bronze
    # ----------------------------------------------------------------
    df = df[[c for c in df.columns if c != "_fetched_at"]]

    save_silver(df, "campaigns")
    return df


if __name__ == "__main__":
    df = build_campaigns()
    show = [c for c in ["line_item_id", "line_item_name", "campaign_name", "advertiser", "line_start", "line_end"] if c in df.columns]
    print(df[show].head(5).to_string())
