"""
Gold — fact_play_logs

Slim fact table z play_logs_enr: tylko klucze + miary.
Nazwy i atrybuty sa w dim_* — nie duplikujemy ich tutaj.

Klucze (FK do dim):
  date_key              -> dim_date.date_key
  play_log_player_id    -> dim_player.play_log_player_id
  frame_id              -> dim_screen.frame_id
  reservation_id        -> dim_line_item.reservation_id
  campaign_id           -> dim_campaign.campaign_id
  content_id            -> dim_content.content_id

Miary:
  emisje, Impresje, Duration

Wykluczone: kampanie z EXCLUDED_CAMPAIGN_IDS (autopromocja).
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
from Pipeline.gold.utils import read_silver, save_gold, EXCLUDED_CAMPAIGN_IDS

GOLD_DIR = Path(__file__).resolve().parent.parent.parent / "Data" / "gold"


def build_fact_play_logs():
    df = read_silver("play_logs_enriched")

    keep = [
        "DateEnd",        # -> date_key
        "PlayerID",       # -> play_log_player_id
        "FrameID",        # -> frame_id
        "DisplayUnitID",  # -> display_unit_id
        "CampID",         # -> reservation_id
        "AdCopyId",       # -> content_id
        "timeslot",
        "contract_id",
        # miary
        "emisje",
        "Impresje",
        "Duration",
    ]
    df = df[[c for c in keep if c in df.columns]].copy()

    # Typy
    for col in ["PlayerID", "FrameID", "DisplayUnitID", "AdCopyId", "CampID"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    df["timeslot"] = pd.to_numeric(df["timeslot"], errors="coerce").astype("Int64")

    # Spójne nazwy kluczy
    df = df.rename(columns={
        "DateEnd":       "date_key",
        "PlayerID":      "play_log_player_id",
        "FrameID":       "frame_id",
        "DisplayUnitID": "display_unit_id",
        "CampID":        "reservation_id",
        "AdCopyId":      "content_id",
    })

    # ------------------------------------------------------------------
    # Dociagnij campaign_id z dim_line_item (reservation_id -> campaign_id)
    # ------------------------------------------------------------------
    li_path = GOLD_DIR / "dim_line_item.parquet"
    if not li_path.exists():
        from Pipeline.gold.build_dim_line_item import build_dim_line_item
        build_dim_line_item()

    res_camp = pd.read_parquet(li_path, columns=["reservation_id", "campaign_id"])
    res_camp = res_camp.dropna(subset=["reservation_id"]).drop_duplicates(subset=["reservation_id"])
    res_camp["reservation_id"] = res_camp["reservation_id"].astype("Int64")
    res_camp["campaign_id"]    = res_camp["campaign_id"].astype("Int64")

    df = df.merge(res_camp, on="reservation_id", how="left")

    # ------------------------------------------------------------------
    # Wyklucz autopromocje
    # ------------------------------------------------------------------
    before = len(df)
    df = df[~df["campaign_id"].isin(EXCLUDED_CAMPAIGN_IDS)]
    excluded = before - len(df)
    if excluded:
        print(f"  Wykluczono autopromocja: {excluded:,} wierszy")

    print(f"  Wierszy: {len(df):,}")
    save_gold(df, "fact_play_logs")


if __name__ == "__main__":
    build_fact_play_logs()
