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
from Pipeline.gold.utils import (
    read_silver, save_gold, SERWISOWY_CAMPAIGN_IDS, get_single_panel_campaign_ids,
    EXCLUDED_CAMPAIGN_IDS, FORCE_SINGLE_PANEL_CAMPAIGN_IDS, MANUAL_RESERVATION_OVERRIDES,
)

CAMP_COLS = [
    "campaign_id", "campaign_name", "campaign_status",
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

    # Kampanie twardo wykluczone (stare/nieaktualne dane) — patrz utils.py
    df = df[~df["campaign_id"].isin(EXCLUDED_CAMPAIGN_IDS)]

    # Doloz syntetyczne "kampanie" dla rezerwacji bez zadnego bookingu w Broadsign
    # (brak proposal_id w zrodle) -- patrz MANUAL_RESERVATION_OVERRIDES w utils.py.
    synthetic = [
        {
            "campaign_id":  ov["campaign_id"],
            "campaign_name": ov["campaign_name"],
            "advertiser":    ov.get("advertiser"),
            "client_name":   ov.get("client_name"),
        }
        for ov in MANUAL_RESERVATION_OVERRIDES.values()
    ]
    if synthetic:
        df = pd.concat([df, pd.DataFrame(synthetic)], ignore_index=True)
        df["campaign_id"] = pd.to_numeric(df["campaign_id"], errors="coerce").astype("Int64")

    # Flaga is_serwisowy: 1=autopromocja/serwisowe, 2=single-panel (test/diagnostyczne)
    single_panel = get_single_panel_campaign_ids() | FORCE_SINGLE_PANEL_CAMPAIGN_IDS
    df["is_serwisowy"] = df["campaign_id"].map(
        lambda x: 1 if x in SERWISOWY_CAMPAIGN_IDS else (2 if x in single_panel else 0)
    ).astype("int8")
    print(f"  Serwisowe (1): {(df['is_serwisowy']==1).sum()}")
    print(f"  Single-panel (2): {(df['is_serwisowy']==2).sum()}")

    for col in ["client_id", "owner_user_id"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    df = df.sort_values("campaign_id")

    print(f"  Kampanie: {len(df)}")
    save_gold(df, "dim_campaign")


if __name__ == "__main__":
    build_dim_campaign()
