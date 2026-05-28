"""
Gold — fact_campaign_budget

Granularność: kampania × lineitem × reservation × player × dzień

Logika alokacji kosztu:
  1. Reservation → line_item przez ctrl_reservations_v22.proposal_line_item_id
  2. Daty: line_start → line_end (planowane, niezależnie od logów)
  3. Playerzy: empiryczni z play_logs (które PlayerID emitowały dla danego CampID)
     Fallback: jeśli brak logów → screen_count wirtualnych slotów (bez nazwy)
  4. Koszt (line_price): line_price / n_days / n_players  = daily_cost_per_player_line
  5. Koszt (campaign_price): campaign_price_allocated / n_days / n_players

Wynik per wiersz:
  campaign_id, line_item_id, reservation_id, player_id (lub None), date
  → daily_cost_line, daily_cost_campaign
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
import numpy as np
from Pipeline.gold.utils import read_bronze, read_silver, save_gold


def build_fact_budget():
    # ------------------------------------------------------------------
    # 1. Reservation → line_item mapping (z bronze v22)
    # ------------------------------------------------------------------
    try:
        res_v22 = read_bronze("ctrl_reservations_v22")[
            ["id", "proposal_line_item_id", "proposal_id"]
        ].copy()
    except FileNotFoundError:
        print("  WARN: ctrl_reservations_v22.parquet nie istnieje — uruchom najpierw bronze")
        return

    res_v22 = res_v22.rename(columns={"id": "reservation_id"})
    res_v22["proposal_line_item_id"] = pd.to_numeric(
        res_v22["proposal_line_item_id"], errors="coerce"
    ).astype("Int64")
    res_v22 = res_v22.dropna(subset=["proposal_line_item_id"])
    print(f"  Reservations z proposal_line_item_id: {len(res_v22)}")

    # ------------------------------------------------------------------
    # 2. Dane lineitemu z dim_campaign (ceny, daty, screen_count)
    # ------------------------------------------------------------------
    try:
        dim_camp = pd.read_parquet(
            Path(__file__).resolve().parent.parent.parent / "Data" / "gold" / "dim_campaign.parquet"
        )
    except FileNotFoundError:
        # Zbuduj on-the-fly z silver
        from Pipeline.gold.build_dims import build_dim_campaign
        build_dim_campaign()
        dim_camp = pd.read_parquet(
            Path(__file__).resolve().parent.parent.parent / "Data" / "gold" / "dim_campaign.parquet"
        )

    dim_camp["line_item_id"] = pd.to_numeric(dim_camp["line_item_id"], errors="coerce").astype("Int64")
    camp_cols = [
        "line_item_id", "campaign_id",
        "line_price", "campaign_price_allocated",
        "line_start", "line_end", "line_days",
        "screen_count",
    ]
    dim_camp = dim_camp[[c for c in camp_cols if c in dim_camp.columns]]

    # ------------------------------------------------------------------
    # 3. Połącz reservation z lineitem
    # ------------------------------------------------------------------
    merged = res_v22.merge(
        dim_camp,
        left_on="proposal_line_item_id",
        right_on="line_item_id",
        how="inner",
    )
    print(f"  Reservation-lineitem matches: {len(merged)}")

    # ------------------------------------------------------------------
    # 4. Playerzy per reservation z play_logs
    # ------------------------------------------------------------------
    logs = read_bronze("play_logs")[["CampID", "PlayerID"]].copy()
    logs["CampID"]    = pd.to_numeric(logs["CampID"],    errors="coerce").astype("Int64")
    logs["PlayerID"]  = pd.to_numeric(logs["PlayerID"],  errors="coerce").astype("Int64")
    logs = logs.dropna()

    # Unikalne (CampID, PlayerID) pary
    res_players = (
        logs.groupby("CampID")["PlayerID"]
        .apply(lambda x: sorted(x.unique().tolist()))
        .reset_index()
        .rename(columns={"CampID": "reservation_id", "PlayerID": "player_ids"})
    )
    res_players["reservation_id"] = res_players["reservation_id"].astype("Int64")

    # ------------------------------------------------------------------
    # 5. Generuj wiersze: reservation × date × player
    # ------------------------------------------------------------------
    rows = []

    for _, r in merged.iterrows():
        res_id     = r["reservation_id"]
        line_id    = r["line_item_id"]
        camp_id    = r.get("campaign_id", None)
        line_price = r.get("line_price", 0) or 0
        camp_price = r.get("campaign_price_allocated", 0) or 0
        n_days     = max(int(r.get("line_days", 1) or 1), 1)
        screen_cnt = max(int(r.get("screen_count", 1) or 1), 1)

        # Daty
        try:
            date_range = pd.date_range(
                start=r["line_start"], end=r["line_end"], freq="D"
            )
        except Exception:
            continue
        if len(date_range) == 0:
            continue

        # Playerzy
        player_match = res_players[res_players["reservation_id"] == res_id]
        if len(player_match) > 0:
            player_list = player_match.iloc[0]["player_ids"]
        else:
            # Brak logów — wirtualne sloty (player_id = None, screen_count slotów)
            player_list = [None] * screen_cnt

        n_players = max(len(player_list), 1)

        daily_line = line_price / n_days / n_players
        daily_camp = camp_price / n_days / n_players if camp_price else None

        for day in date_range:
            day_str = day.strftime("%Y-%m-%d")
            for pid in player_list:
                rows.append({
                    "campaign_id":    camp_id,
                    "line_item_id":   line_id,
                    "reservation_id": res_id,
                    "player_id":      pid,
                    "date":           day_str,
                    "daily_cost_line":     round(daily_line, 4),
                    "daily_cost_campaign": round(daily_camp, 4) if daily_camp is not None else None,
                    "n_days":         n_days,
                    "n_players":      n_players,
                })

    if not rows:
        print("  WARN: brak wierszy do zapisania")
        return

    fact = pd.DataFrame(rows)
    fact["campaign_id"]    = pd.to_numeric(fact["campaign_id"],    errors="coerce").astype("Int64")
    fact["line_item_id"]   = pd.to_numeric(fact["line_item_id"],   errors="coerce").astype("Int64")
    fact["reservation_id"] = pd.to_numeric(fact["reservation_id"], errors="coerce").astype("Int64")
    fact["player_id"]      = pd.to_numeric(fact["player_id"],      errors="coerce").astype("Int64")

    print(f"  Wierszy fact_budget: {len(fact):,}")
    print(f"  Z player_id: {fact['player_id'].notna().sum():,} / {len(fact):,}")
    save_gold(fact, "fact_campaign_budget")


if __name__ == "__main__":
    build_fact_budget()
