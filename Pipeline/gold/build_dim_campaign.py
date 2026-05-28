"""
Gold — dim_campaign

Jeden wiersz = jedna kampania (proposal).
Zawiera atrybuty na poziomie kampanii: ceny, daty, owner, klient.

Klucz: campaign_id → fact_campaign_budget.campaign_id
                    → dim_line_item.campaign_id
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
from Pipeline.gold.utils import read_silver, save_gold, EXCLUDED_CAMPAIGN_IDS

CAMP_COLS = [
    "campaign_id", "campaign_name",
    "advertiser", "client_id", "client_name",
    "campaign_price", "campaign_suggested_price", "campaign_discount",
    "campaign_start", "campaign_end",
    "owner_email", "owner_user_id", "campaign_owner_name",
    "contract_id", "contract_number",
]


def build_dim_campaign():
    camp = read_silver("campaigns")

    df = camp[[c for c in CAMP_COLS if c in camp.columns]].copy()
    df["campaign_id"] = pd.to_numeric(df["campaign_id"], errors="coerce").astype("Int64")

    # Jeden wiersz per kampania
    df = df.drop_duplicates(subset=["campaign_id"])

    # Wyklucz autopromocję i podobne
    df = df[~df["campaign_id"].isin(EXCLUDED_CAMPAIGN_IDS)]

    for col in ["client_id", "owner_user_id"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    df = df.sort_values("campaign_id")

    print(f"  Kampanie: {len(df)}")
    save_gold(df, "dim_campaign")


if __name__ == "__main__":
    build_dim_campaign()
