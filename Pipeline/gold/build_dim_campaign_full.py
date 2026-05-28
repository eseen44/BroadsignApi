"""
Gold — dim_campaign_full

Jedna tabela łącząca: kampania × lineitem × rezerwacja.

Grain: jeden wiersz = jeden line item (z przypisaną rezerwacją jeśli istnieje).
       Dla każdej kampanii gdzie campaign_price ≠ Σ line_price
       dodawany jest jeden wiersz wyrównujący (is_adjustment=True)
       o wartości = campaign_price - Σ line_price.

Źródła:
  silver.campaigns          → kampanie + line itemy (Direct API)
  bronze.ctrl_reservations_v22  → proposal_line_item_id (klucz do lineitemu)
  bronze.ctrl_reservations  → daty, stan, saturacja rezerwacji

Klucze dla joinów z fact tables:
  line_item_id   → fact_campaign_budget.line_item_id
  reservation_id → fact_campaign_budget.reservation_id
                   fact_play_logs.CampID
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
import numpy as np
from Pipeline.gold.utils import read_bronze, read_silver, save_gold


def build_dim_campaign_full():

    # ------------------------------------------------------------------
    # 1. Silver campaigns: lineitem + kampania + owner
    # ------------------------------------------------------------------
    camp = read_silver("campaigns")

    li_cols = [
        "line_item_id", "campaign_id",
        "line_item_name", "campaign_name",
        "advertiser", "client_id", "client_name",
        "type_of_buy", "slot_duration", "screen_count", "group_count",
        "status_id", "status_name", "is_preemptible",
        "line_price", "line_suggested_price",
        "campaign_price", "campaign_suggested_price", "campaign_discount",
        "buy_saturation", "buy_bs_saturation", "buy_sov", "buy_budget",
        "perf_expected_repetitions", "perf_actual_repetitions",
        "perf_expected_impressions", "perf_actual_impressions",
        "line_start", "line_end",
        "campaign_start", "campaign_end",
        "owner_email", "owner_user_id",
        "contract_id", "contract_number",
        "campaign_owner_name",
    ]
    camp = camp[[c for c in li_cols if c in camp.columns]].copy()
    camp["line_item_id"] = pd.to_numeric(camp["line_item_id"], errors="coerce").astype("Int64")
    camp["campaign_id"]  = pd.to_numeric(camp["campaign_id"],  errors="coerce").astype("Int64")

    # Liczba dni lineitemu
    camp["line_start"] = pd.to_datetime(camp["line_start"], errors="coerce")
    camp["line_end"]   = pd.to_datetime(camp["line_end"],   errors="coerce")
    camp["line_days"]  = (camp["line_end"] - camp["line_start"]).dt.days + 1
    camp["line_days"]  = camp["line_days"].clip(lower=1)

    # ------------------------------------------------------------------
    # 2. Rezerwacje z v22 (proposal_line_item_id → klucz do lineitemu)
    # ------------------------------------------------------------------
    res22 = read_bronze("ctrl_reservations_v22")[
        ["id", "proposal_line_item_id", "booking_state"]
    ].copy()
    res22 = res22.rename(columns={"id": "reservation_id"})
    res22["proposal_line_item_id"] = pd.to_numeric(
        res22["proposal_line_item_id"], errors="coerce"
    ).astype("Int64")
    res22 = res22.dropna(subset=["proposal_line_item_id"])

    # Pełne dane rezerwacji (daty, stan, saturacja)
    res_full = read_bronze("ctrl_reservations")[
        ["id", "name", "start_date", "end_date", "state", "saturation", "duration_msec", "active"]
    ].copy()
    res_full = res_full.rename(columns={
        "id":           "reservation_id",
        "name":         "reservation_name",
        "start_date":   "reservation_start",
        "end_date":     "reservation_end",
        "state":        "reservation_state",
    })
    res_full["reservation_id"] = pd.to_numeric(res_full["reservation_id"], errors="coerce").astype("Int64")

    # Połącz v22 z pełnymi danymi
    res = res22.merge(res_full, on="reservation_id", how="left")

    # ------------------------------------------------------------------
    # 3. Główny join: lineitem → rezerwacja (left, żeby zostały LI bez rez.)
    # ------------------------------------------------------------------
    df = camp.merge(
        res.rename(columns={"proposal_line_item_id": "line_item_id"}),
        on="line_item_id",
        how="left",
    )

    df["is_adjustment"] = False

    print(f"  Line items:               {len(camp)}")
    print(f"  Z rezerwacją:             {df['reservation_id'].notna().sum()} "
          f"({df['reservation_id'].notna().mean():.1%})")
    print(f"  Bez rezerwacji:           {df['reservation_id'].isna().sum()}")

    # ------------------------------------------------------------------
    # 4. Wiersze wyrównujące (adjustment) per kampania
    # ------------------------------------------------------------------
    # Suma line_price per kampania
    camp_sum = df.groupby("campaign_id").agg(
        sum_line_prices=("line_price", "sum"),
        campaign_price=("campaign_price", "first"),
        campaign_name=("campaign_name", "first"),
        advertiser=("advertiser", "first"),
        client_id=("client_id", "first"),
        client_name=("client_name", "first"),
        campaign_start=("campaign_start", "first"),
        campaign_end=("campaign_end", "first"),
        owner_email=("owner_email", "first"),
        owner_user_id=("owner_user_id", "first"),
        campaign_owner_name=("campaign_owner_name", "first") if "campaign_owner_name" in df.columns else ("campaign_name", "first"),
        contract_id=("contract_id", "first"),
    ).reset_index()

    camp_sum["adjustment_value"] = camp_sum["campaign_price"] - camp_sum["sum_line_prices"]

    # Dodaj wiersz wyrównujący tylko gdy różnica > 1 PLN (w dowolną stronę)
    adj_rows = camp_sum[camp_sum["adjustment_value"].abs() > 1].copy()
    print(f"  Kampanie wymagające korekty: {len(adj_rows)} / {len(camp_sum)}")

    if len(adj_rows) > 0:
        adj = pd.DataFrame({
            "campaign_id":       adj_rows["campaign_id"].values,
            "campaign_name":     adj_rows["campaign_name"].values,
            "advertiser":        adj_rows["advertiser"].values,
            "client_id":         adj_rows["client_id"].values,
            "client_name":       adj_rows["client_name"].values,
            "campaign_start":    adj_rows["campaign_start"].values,
            "campaign_end":      adj_rows["campaign_end"].values,
            "owner_email":       adj_rows["owner_email"].values,
            "owner_user_id":     adj_rows["owner_user_id"].values,
            "campaign_price":    adj_rows["campaign_price"].values,
            "contract_id":       adj_rows["contract_id"].values,
            # Pola specyficzne dla wiersza korygującego
            "line_item_id":      pd.array([pd.NA] * len(adj_rows), dtype="Int64"),
            "line_item_name":    "[korekta kampanii]",
            "line_price":        adj_rows["adjustment_value"].values,
            "line_suggested_price": 0.0,
            "line_days":         pd.array([pd.NA] * len(adj_rows), dtype="Int64"),
            "screen_count":      pd.array([pd.NA] * len(adj_rows), dtype="Int64"),
            "is_adjustment":     True,
            "reservation_id":    pd.array([pd.NA] * len(adj_rows), dtype="Int64"),
        })
        # Wyrównaj kolumny przed concat żeby uniknąć FutureWarning
        for col in df.columns:
            if col not in adj.columns:
                adj[col] = pd.NA
        df = pd.concat([df, adj[df.columns]], ignore_index=True)

    # ------------------------------------------------------------------
    # 5. Typy i porządki
    # ------------------------------------------------------------------
    for col in ["line_item_id", "campaign_id", "reservation_id", "owner_user_id",
                "screen_count", "group_count", "line_days"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in ["line_start", "line_end"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")

    # ------------------------------------------------------------------
    # 6. Walidacja: suma per kampania powinna = campaign_price
    # ------------------------------------------------------------------
    check = df.groupby("campaign_id").agg(
        camp_price=("campaign_price", "first"),
        sum_lines=("line_price", "sum"),
    )
    check["ok"] = (check["camp_price"] - check["sum_lines"]).abs() < 1
    print(f"  Kampanie spięte po korekcie: {check['ok'].sum()} / {len(check)} "
          f"({check['ok'].mean():.1%})")

    df = df.sort_values(["campaign_id", "line_item_id"], na_position="last")

    save_gold(df, "dim_campaign_full")


if __name__ == "__main__":
    build_dim_campaign_full()
