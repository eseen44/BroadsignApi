"""
Gold — fact_campaign_budget

Granularność: lineitem × rezerwacja × player × dzień

Źródło danych lineitemu: dim_campaign_full (zawiera już line_price, daty, screen_count,
reservation_id). Wiersze is_adjustment=True (korekty kampanijne) nie mają rezerwacji
ani playerów — trafiają do fact jako wiersze z player_id=NULL.

Logika alokacji kosztu:
  1. Daty: line_start → line_end (planowane, niezależnie od logów)
  2. Playerzy z play_logs: które PlayerID emitowały dla CampID = reservation_id
     Fallback gdy brak logów: screen_count wirtualnych slotów (player_id=NULL)
  3. line_price / n_days / n_players  → daily_cost_line per wiersz

Wynik per wiersz:
  campaign_id, line_item_id, reservation_id, player_id, date
  → daily_cost_line
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
from Pipeline.gold.utils import read_bronze, save_gold

GOLD_DIR = Path(__file__).resolve().parent.parent.parent / "Data" / "gold"


def build_fact_budget():

    # ------------------------------------------------------------------
    # 1. dim_campaign_full jako źródło lineitemów
    # ------------------------------------------------------------------
    dcf_path = GOLD_DIR / "dim_campaign_full.parquet"
    if not dcf_path.exists():
        from Pipeline.gold.build_dim_campaign_full import build_dim_campaign_full
        build_dim_campaign_full()

    dcf = pd.read_parquet(dcf_path, columns=[
        "campaign_id", "line_item_id", "reservation_id",
        "line_price", "line_start", "line_end", "line_days",
        "screen_count", "is_adjustment",
    ])
    dcf["line_item_id"]   = pd.to_numeric(dcf["line_item_id"],   errors="coerce").astype("Int64")
    dcf["campaign_id"]    = pd.to_numeric(dcf["campaign_id"],    errors="coerce").astype("Int64")
    dcf["reservation_id"] = pd.to_numeric(dcf["reservation_id"], errors="coerce").astype("Int64")

    print(f"  dim_campaign_full wierszy:   {len(dcf)}")
    print(f"    rzeczywiste lineitemy:     {(~dcf['is_adjustment']).sum()}")
    print(f"    wiersze korygujące:        {dcf['is_adjustment'].sum()}")

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
    # 3. Generuj wiersze: lineitem × date × player
    # ------------------------------------------------------------------
    rows = []

    for _, r in dcf.iterrows():
        line_id    = r["line_item_id"]
        camp_id    = r["campaign_id"]
        res_id     = r["reservation_id"]
        line_price = float(r["line_price"]) if pd.notna(r["line_price"]) else 0.0
        is_adj     = bool(r["is_adjustment"])

        # Daty
        try:
            date_range = pd.date_range(
                start=r["line_start"], end=r["line_end"], freq="D"
            )
        except Exception:
            date_range = []

        # Wiersze korygujące: jeden wiersz bez daty i bez playera
        if is_adj or len(date_range) == 0:
            rows.append({
                "campaign_id":     camp_id,
                "line_item_id":    line_id,
                "reservation_id":  res_id,
                "player_id":       pd.NA,
                "date":            pd.NA,
                "daily_cost_line": line_price,   # całość — brak rozłożenia
                "n_days":          pd.NA,
                "n_players":       pd.NA,
                "is_adjustment":   is_adj,
            })
            continue

        n_days = max(int(r["line_days"]) if pd.notna(r["line_days"]) else len(date_range), 1)

        # Playerzy z logów
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
                    "is_adjustment":   False,
                })

    if not rows:
        print("  WARN: brak wierszy")
        return

    fact = pd.DataFrame(rows)

    for col in ["campaign_id", "line_item_id", "reservation_id", "player_id",
                "n_days", "n_players"]:
        fact[col] = pd.to_numeric(fact[col], errors="coerce").astype("Int64")

    fact["daily_cost_line"] = fact["daily_cost_line"].astype("float64")

    total = len(fact)
    with_player = fact["player_id"].notna().sum()
    adj_rows    = fact["is_adjustment"].sum()
    print(f"  Wierszy łącznie:             {total:,}")
    print(f"    z player_id:               {with_player:,} ({with_player/total:.1%})")
    print(f"    wiersze korygujące:        {adj_rows:,}")

    save_gold(fact, "fact_campaign_budget")


if __name__ == "__main__":
    build_fact_budget()
