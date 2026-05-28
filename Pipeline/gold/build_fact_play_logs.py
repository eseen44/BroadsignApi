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
from Pipeline.gold.utils import read_silver, save_gold, EXCLUDED_CAMPAIGN_IDS, EXCLUDED_RESERVATION_IDS

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
    # Dociagnij campaign_id z silver (pelne dane, bez filtrowania kampanii)
    # zeby wykluczenie dzialalo tez dla kampanii wyjetych z dim_line_item
    # ------------------------------------------------------------------
    from Pipeline.gold.utils import read_silver as _read_silver
    res_camp = _read_silver("campaigns")[["line_item_id", "campaign_id"]].copy()
    res_camp["line_item_id"] = pd.to_numeric(res_camp["line_item_id"], errors="coerce").astype("Int64")
    res_camp["campaign_id"]  = pd.to_numeric(res_camp["campaign_id"],  errors="coerce").astype("Int64")

    # Potrzebujemy reservation_id -> campaign_id przez ctrl_reservations_v22
    from Pipeline.gold.utils import read_bronze as _read_bronze
    res22 = _read_bronze("ctrl_reservations_v22")[["id", "proposal_line_item_id"]].copy()
    res22 = res22.rename(columns={"id": "reservation_id", "proposal_line_item_id": "line_item_id"})
    res22["reservation_id"] = pd.to_numeric(res22["reservation_id"], errors="coerce").astype("Int64")
    res22["line_item_id"]   = pd.to_numeric(res22["line_item_id"],   errors="coerce").astype("Int64")
    res22 = res22.dropna()

    res_camp = res22.merge(res_camp, on="line_item_id", how="left")
    res_camp = res_camp[["reservation_id", "campaign_id"]].dropna(subset=["reservation_id"])
    res_camp = res_camp.drop_duplicates(subset=["reservation_id"])

    df = df.merge(res_camp, on="reservation_id", how="left")

    # ------------------------------------------------------------------
    # Wyklucz po campaign_id (autopromocja, czas dla metra...)
    # ------------------------------------------------------------------
    before = len(df)
    df = df[~df["campaign_id"].isin(EXCLUDED_CAMPAIGN_IDS)]
    excluded = before - len(df)
    if excluded:
        print(f"  Wykluczono wg campaign_id: {excluded:,} wierszy")

    # Wyklucz po reservation_id (explicite wymienione)
    before = len(df)
    df = df[~df["reservation_id"].isin(EXCLUDED_RESERVATION_IDS)]
    excluded = before - len(df)
    if excluded:
        print(f"  Wykluczono wg reservation_id: {excluded:,} wierszy")

    # Wyklucz wszystkie bez campaign_id (Control-only reservations, stare bez v22)
    before = len(df)
    df = df[df["campaign_id"].notna()]
    print(f"  Wykluczono NULL campaign_id:  {before - len(df):,} wierszy")

    print(f"  Wierszy: {len(df):,}")
    save_gold(df, "fact_play_logs")


if __name__ == "__main__":
    build_fact_play_logs()
