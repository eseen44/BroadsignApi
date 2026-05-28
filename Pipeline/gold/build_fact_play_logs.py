"""
Gold — fact_play_logs

Slim fact table z play_logs_enr: tylko klucze + miary.
Nazwy i atrybuty są w dim_* — nie duplikujemy ich tutaj.

Klucze (FK do dim):
  DateEnd       → dim_date.date_key
  PlayerID      → dim_player.play_log_player_id
  FrameID       → dim_screen.frame_id
  CampID        → dim_reservation_campaign.reservation_id  (przez reservation)
  AdCopyId      → dim_content.content_id

Miary:
  emisje, Impresje, Duration
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
from Pipeline.gold.utils import read_silver, read_bronze, save_gold


def build_fact_play_logs():
    df = read_silver("play_logs_enriched")

    # Slim — tylko klucze i miary
    keep = [
        "DateEnd",        # → date_key
        "PlayerID",       # → play_log_player_id
        "FrameID",        # → frame_id
        "DisplayUnitID",  # → display_unit_id
        "CampID",         # → reservation_id
        "AdCopyId",       # → content_id
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

    # Spójne nazwy kluczy (matching z dim tables)
    df = df.rename(columns={
        "DateEnd":       "date_key",
        "PlayerID":      "play_log_player_id",
        "FrameID":       "frame_id",
        "DisplayUnitID": "display_unit_id",
        "CampID":        "reservation_id",
        "AdCopyId":      "content_id",
    })

    save_gold(df, "fact_play_logs")


if __name__ == "__main__":
    build_fact_play_logs()
