"""
Gold — dim_line_item

Jeden wiersz = jeden line item z przypisaną rezerwacją (jesli istnieje).
Zawiera atrybuty na poziomie lineitemu: ceny, daty, ekrany, status, rezerwacja.

Klucz: line_item_id → fact_campaign_budget.line_item_id
FK:    campaign_id  → dim_campaign.campaign_id
       reservation_id → fact_play_logs.reservation_id
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
from Pipeline.gold.utils import read_bronze, read_silver, save_gold, EXCLUDED_CAMPAIGN_IDS

LI_COLS = [
    "line_item_id", "campaign_id",
    "line_item_name",
    "type_of_buy", "slot_duration", "screen_count", "group_count",
    "status_id", "status_name", "is_preemptible",
    "line_price", "line_suggested_price",
    "buy_saturation", "buy_bs_saturation", "buy_sov", "buy_budget",
    "perf_expected_repetitions", "perf_actual_repetitions",
    "perf_expected_impressions", "perf_actual_impressions",
    "line_start", "line_end",
]


def build_dim_line_item():

    # ------------------------------------------------------------------
    # 1. Line itemy z silver
    # ------------------------------------------------------------------
    camp = read_silver("campaigns")

    df = camp[[c for c in LI_COLS if c in camp.columns]].copy()
    df["line_item_id"] = pd.to_numeric(df["line_item_id"], errors="coerce").astype("Int64")
    df["campaign_id"]  = pd.to_numeric(df["campaign_id"],  errors="coerce").astype("Int64")

    # Wyklucz autopromocję i podobne
    df = df[~df["campaign_id"].isin(EXCLUDED_CAMPAIGN_IDS)]

    # Liczba dni
    df["line_start"] = pd.to_datetime(df["line_start"], errors="coerce")
    df["line_end"]   = pd.to_datetime(df["line_end"],   errors="coerce")
    df["line_days"]  = (df["line_end"] - df["line_start"]).dt.days + 1
    df["line_days"]  = df["line_days"].clip(lower=1)

    # ------------------------------------------------------------------
    # 2. Rezerwacje (v22 + pelne dane)
    # ------------------------------------------------------------------
    res22 = read_bronze("ctrl_reservations_v22")[
        ["id", "proposal_line_item_id", "booking_state"]
    ].copy()
    res22 = res22.rename(columns={"id": "reservation_id"})
    res22["proposal_line_item_id"] = pd.to_numeric(
        res22["proposal_line_item_id"], errors="coerce"
    ).astype("Int64")
    res22 = res22.dropna(subset=["proposal_line_item_id"])

    res_full = read_bronze("ctrl_reservations")[
        ["id", "name", "start_date", "end_date", "state", "saturation", "duration_msec", "active"]
    ].copy()
    res_full = res_full.rename(columns={
        "id":         "reservation_id",
        "name":       "reservation_name",
        "start_date": "reservation_start",
        "end_date":   "reservation_end",
        "state":      "reservation_state",
    })
    res_full["reservation_id"] = pd.to_numeric(
        res_full["reservation_id"], errors="coerce"
    ).astype("Int64")

    res = res22.merge(res_full, on="reservation_id", how="left")

    # ------------------------------------------------------------------
    # 3. Join: lineitem -> rezerwacja
    # ------------------------------------------------------------------
    df = df.merge(
        res.rename(columns={"proposal_line_item_id": "line_item_id"}),
        on="line_item_id",
        how="left",
    )

    # ------------------------------------------------------------------
    # 4. Typy i formatowanie
    # ------------------------------------------------------------------
    for col in ["line_start", "line_end"]:
        df[col] = df[col].dt.strftime("%Y-%m-%d")

    for col in ["line_item_id", "campaign_id", "reservation_id",
                "screen_count", "group_count", "line_days"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    df = df.sort_values(["campaign_id", "line_item_id"])

    print(f"  Line items:       {len(df)}")
    print(f"  Z rezerwacja:     {df['reservation_id'].notna().sum()} "
          f"({df['reservation_id'].notna().mean():.1%})")
    print(f"  Bez rezerwacji:   {df['reservation_id'].isna().sum()}")
    save_gold(df, "dim_line_item")


if __name__ == "__main__":
    build_dim_line_item()
