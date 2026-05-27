"""
Silver: players_full = ctrl_players × ctrl_display_units

Klucz joinu: ctrl_players.target_display_unit_id → ctrl_display_units.id

Uwagi:
  play_logs.PlayerID     = ctrl_players.claimed_resource_id  (NIE .id)
  play_logs.DisplayUnitID = ctrl_display_units.id
  ctrl_players.target_display_unit_id → ctrl_display_units.id  (właściwy link)
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
from Pipeline.silver.utils import read_bronze, save_silver


def build_players() -> pd.DataFrame:
    players = read_bronze("ctrl_players")
    du = read_bronze("ctrl_display_units")

    # ctrl_players — kluczowe kolumny
    player_cols = {
        "id":                     "player_id",
        "claimed_resource_id":    "play_log_player_id",  # = play_logs.PlayerID
        "name":                   "player_name",
        "hostname":               "hostname",
        "active":                 "player_active",
        "licensed":               "licensed",
        "nscreens":               "nscreens",
        "target_display_unit_id": "display_unit_id",     # join key → ctrl_display_units.id
    }
    existing_p = {k: v for k, v in player_cols.items() if k in players.columns}
    players_slim = players[list(existing_p.keys())].rename(columns=existing_p)

    # Zamień 0 na NaN w display_unit_id (nieprzypisane playery)
    players_slim["display_unit_id"] = players_slim["display_unit_id"].replace(0, pd.NA)

    # ctrl_display_units — kluczowe kolumny
    du_cols = {
        "id":       "display_unit_id",
        "name":     "display_unit_name",
        "address":  "du_address",
        "timezone": "timezone",
        "active":   "du_active",
    }
    existing_du = {k: v for k, v in du_cols.items() if k in du.columns}
    du_slim = du[list(existing_du.keys())].rename(columns=existing_du)

    # Ujednolicenie typów
    players_slim["display_unit_id"] = pd.to_numeric(players_slim["display_unit_id"], errors="coerce").astype("Int64")
    du_slim["display_unit_id"] = pd.to_numeric(du_slim["display_unit_id"], errors="coerce").astype("Int64")

    # JOIN: players → display_units (przez target_display_unit_id)
    df = players_slim.merge(du_slim, on="display_unit_id", how="left")

    df = df[[c for c in df.columns if c != "_fetched_at"]]

    save_silver(df, "players_full")
    return df


if __name__ == "__main__":
    df = build_players()
    print(df[["player_id", "player_name", "display_unit_id", "display_unit_name"]].head(10).to_string())
    print(f"\nPlayers: {len(df)}, matched DU: {df['display_unit_name'].notna().sum()}")
