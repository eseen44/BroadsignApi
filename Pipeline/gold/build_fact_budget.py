"""
Gold — fact_campaign_budget

Granularnosc: lineitem x rezerwacja x player x dzien

Zrodlo: dim_line_item (line_price, daty, screen_count, reservation_id).
Playerzy z play_logs: ktore PlayerID emitowaly dla CampID = reservation_id.
Fallback gdy brak logow: screen_count wirtualnych slotow (player_id=NULL).

Logika alokacji kosztu:
  line_price / n_days / n_players -> daily_cost_line per wiersz

Wykluczone: kampanie z EXCLUDED_CAMPAIGN_IDS (autopromocja).
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
from Pipeline.gold.utils import read_bronze, save_gold, EXCLUDED_CAMPAIGN_IDS

GOLD_DIR = Path(__file__).resolve().parent.parent.parent / "Data" / "gold"


def build_fact_budget():

    # ------------------------------------------------------------------
    # 1. dim_line_item jako zrodlo lineitemow
    # ------------------------------------------------------------------
    li_path = GOLD_DIR / "dim_line_item.parquet"
    if not li_path.exists():
        from Pipeline.gold.build_dim_line_item import build_dim_line_item
        build_dim_line_item()

    dli = pd.read_parquet(li_path, columns=[
        "campaign_id", "line_item_id", "reservation_id",
        "line_price", "line_start", "line_end", "line_days",
        "screen_count",
    ])
    dli["line_item_id"]   = pd.to_numeric(dli["line_item_id"],   errors="coerce").astype("Int64")
    dli["campaign_id"]    = pd.to_numeric(dli["campaign_id"],    errors="coerce").astype("Int64")
    dli["reservation_id"] = pd.to_numeric(dli["reservation_id"], errors="coerce").astype("Int64")

    # Filtr autopromocja (powinien byc juz wyczyszczony w dim_line_item, ale dla pewnosci)
    dli = dli[~dli["campaign_id"].isin(EXCLUDED_CAMPAIGN_IDS)]

    print(f"  dim_line_item wierszy: {len(dli)}")

    # ------------------------------------------------------------------
    # 2. Playerzy per rezerwacja z play_logs
    # ------------------------------------------------------------------
    logs = read_bronze("play_logs")[["CampID", "PlayerID"]].copy()
    logs["CampID"]   = pd.to_numeric(logs["CampID"],   errors="coerce").astype("Int64")
    logs["PlayerID"] = pd.to_numeric(logs["PlayerID"], errors="coerce").astype("Int64")
    logs = logs.dropna()

    res_players = (
        logs.groupby("CampID")["PlayerID"]
        .apply(lambda x: sorted(x.unique().tolist()))
        .reset_index()
        .rename(columns={"CampID": "reservation_id", "PlayerID": "player_ids"})
    )
    res_players["reservation_id"] = res_players["reservation_id"].astype("Int64")

    # ------------------------------------------------------------------
    # 3. Generuj wiersze: lineitem x date x player
    # ------------------------------------------------------------------
    rows = []

    for _, r in dli.iterrows():
        line_id    = r["line_item_id"]
        camp_id    = r["campaign_id"]
        res_id     = r["reservation_id"]
        line_price = float(r["line_price"]) if pd.notna(r["line_price"]) else 0.0

        # Daty
        try:
            date_range = pd.date_range(
                start=r["line_start"], end=r["line_end"], freq="D"
            )
        except Exception:
            date_range = []

        if len(date_range) == 0:
            continue

        n_days = max(int(r["line_days"]) if pd.notna(r["line_days"]) else len(date_range), 1)

        # Playerzy z logow (fallback: screen_count wirtualnych slotow)
        screen_cnt = max(int(r["screen_count"]) if pd.notna(r["screen_count"]) else 1, 1)
        if pd.notna(res_id):
            pm = res_players[res_players["reservation_id"] == res_id]
            player_list = pm.iloc[0]["player_ids"] if len(pm) > 0 else [None] * screen_cnt
        else:
            player_list = [None] * screen_cnt

        n_players = max(len(player_list), 1)
        daily = line_price / n_days / n_players

        for day in date_range:
            day_str = day.strftime("%Y-%m-%d")
            for pid in player_list:
                rows.append({
                    "campaign_id":     camp_id,
                    "line_item_id":    line_id,
                    "reservation_id":  res_id,
                    "player_id":       pid,
                    "date":            day_str,
                    "daily_cost_line": round(daily, 4),
                    "n_days":          n_days,
                    "n_players":       n_players,
                })

    if not rows:
        print("  WARN: brak wierszy")
        return

    fact = pd.DataFrame(rows)

    for col in ["campaign_id", "line_item_id", "reservation_id", "player_id",
                "n_days", "n_players"]:
        fact[col] = pd.to_numeric(fact[col], errors="coerce").astype("Int64")

    fact["daily_cost_line"] = fact["daily_cost_line"].astype("float64")

    total       = len(fact)
    with_player = fact["player_id"].notna().sum()
    print(f"  Wierszy lacznie:        {total:,}")
    print(f"  Z play_log_player_id:   {with_player:,} ({with_player/total:.1%})")

    fact = fact.rename(columns={
        "player_id": "play_log_player_id",
        "date":      "date_key",
    })

    save_gold(fact, "fact_campaign_budget")


if __name__ == "__main__":
    build_fact_budget()
