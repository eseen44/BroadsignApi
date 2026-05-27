"""
Silver: screens_full = screens × screens_frames_mapping

Jeden wiersz = jeden frame (każdy screen może mieć wiele frame'ów).
Klucz joinu: SFM.group_id → screens.id  (NIE SFM.screen_id!)

Uwagi o schemacie SFM:
  frame_id   = ID slotu emisji = play_logs.FrameID
  screen_id  = ID panelu fizycznego (sub-wyswietlacz w stacji) - przemianowany na panel_id
  group_id   = ID stacji = screens.id w Direct API  <- właściwy klucz joinu
  group_uuid = UUID stacji = screens.uuid

Ekrany bez screen_name (~79 frame'ow):
  Stacje B9D/B18D (billboardy), StroerTV, BIURO — sa w ctrl_display_units (Control API)
  ale NIE sa w Direct API digital inventory (tam sa tylko 93 ekrany TP).
  Mozna je identyfikowac przez screen_bridge.display_unit_name.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
from Pipeline.silver.utils import read_bronze, save_silver


def build_screens() -> pd.DataFrame:
    screens = read_bronze("screens")
    mapping = read_bronze("screens_frames_mapping")

    # Kolumny z screens
    screen_cols = {
        "id":           "screen_id",
        "name":         "screen_name",
        "address":      "screen_address",
        "resolution":   "resolution",
        "orientation":  "orientation",
    }
    existing = {k: v for k, v in screen_cols.items() if k in screens.columns}
    screens_slim = screens[list(existing.keys())].rename(columns=existing)

    # Kolumny z mapping
    # Uwaga: group_id w SFM = screens.id w Direct API
    #   screen_id w SFM = ID pojedynczego panelu (pod-wyświetlacz w stacji)
    #   frame_id = ID slotu emisji (= play_logs.FrameID)
    map_cols = {
        "frame_id":    "frame_id",
        "screen_id":   "panel_id",    # panel ID (sub-screen), nie = screens.id
        "screen_uuid": "panel_uuid",
        "group_id":    "screen_id",   # to jest właściwy screens.id z Direct API
        "group_uuid":  "screen_uuid",
    }
    existing_map = {k: v for k, v in map_cols.items() if k in mapping.columns}
    mapping_slim = mapping[list(existing_map.keys())].rename(columns=existing_map)

    # Ujednolicenie typów przed joiniem
    mapping_slim["screen_id"] = pd.to_numeric(mapping_slim["screen_id"], errors="coerce").astype("Int64")
    screens_slim["screen_id"] = pd.to_numeric(screens_slim["screen_id"], errors="coerce").astype("Int64")

    # Dedup — API zwraca czasem identyczne wiersze; jeden frame = jeden wiersz
    before = len(mapping_slim)
    mapping_slim = mapping_slim.drop_duplicates(subset=["frame_id"])
    if len(mapping_slim) < before:
        print(f"  Usunieto {before - len(mapping_slim)} zduplikowanych frame_id z SFM")

    # JOIN: mapping → screens (przez group_id=screen_id)
    df = mapping_slim.merge(screens_slim, on="screen_id", how="left")

    df = df[[c for c in df.columns if c != "_fetched_at"]]

    save_silver(df, "screens_full")
    return df


if __name__ == "__main__":
    df = build_screens()
    print(df.head(5).to_string())
    print(f"\nScreens: {df['screen_id'].nunique()}, Frames: {df['frame_id'].nunique()}")
